import {
    AthenaClient,
    StartQueryExecutionCommand,
    GetQueryExecutionCommand,
    GetQueryResultsCommand,
} from '@aws-sdk/client-athena';
import {
    gameEndingDefaults,
    itemStateDefaults,
    matchSettingsDefaults,
    playerSettingsDefaults,
    playerStateDefaults
} from "./defaults.mjs";

const athena = new AthenaClient({ region: 'us-east-2' });

async function runAthenaQuery(query) {

    const startCommand = new StartQueryExecutionCommand({
        QueryString: query,
        QueryExecutionContext: { Database: process.env.GLUE_DATABASE },
        ResultConfiguration: { OutputLocation: process.env.QUERY_OUTPUT_LOCATION },
    });

    const start = await athena.send(startCommand);

    const queryExecutionId = start.QueryExecutionId;
    if (!queryExecutionId) throw new Error('Query start failed');

    // Wait for query completion
    let state = 'QUEUED';
    while (state === 'QUEUED' || state === 'RUNNING') {
        await new Promise((res) => setTimeout(res, 1000));
        const status = await athena.send(new GetQueryExecutionCommand({
            QueryExecutionId: queryExecutionId,
        }));
        state = status.QueryExecution?.Status?.State ?? 'FAILED';

        if (state === 'FAILED') {
            throw new Error(`Query failed: ${status.QueryExecution?.Status?.StateChangeReason}`);
        }
    }

    const result = await athena.send(new GetQueryResultsCommand({
        QueryExecutionId: queryExecutionId,
    }));

    const rows = result.ResultSet?.Rows || [];
    const headers = rows[0]?.Data?.map(d => d.VarCharValue || '') || [];

    return rows.slice(1).map(row =>
        row.Data?.reduce((obj, val, idx) => {
            obj[headers[idx]] = val?.VarCharValue || '';
            return obj;
        }, {})
    );
}

/**
 * @param {Array<{ frame: number, height: number }>} changes - Sorted list of platform height changes.
 * @param {number} currentFrame - The frame to evaluate platform height at.
 * @returns {number|null} - The current platform height or null if no change has occurred yet.
 */
function getPlatformHeightAtFrame(changes, currentFrame) {
    let height = null;

    for (const change of changes) {
        if (Number(change.frame) > Number(currentFrame)) {
            break;
        }
        height = Number(change.platform_height);
    }
    return height;
}

export const handler = async (event) => {

    if (event.requestContext.http.method === 'OPTIONS') {
        return {
            statusCode: 204,
            headers: {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            },
        };
    }

    try {
        const { matchId, frameStart, frameEnd } = JSON.parse(event.body);
        if (!matchId) {
            return { statusCode: 400, body: JSON.stringify({ error: 'Missing matchId' }) };
        }
        if (!frameStart) {
            return { statusCode: 400, body: JSON.stringify({ error: 'Missing frameStart' }) };
        }
        if (!frameEnd) {
            return { statusCode: 400, body: JSON.stringify({ error: 'Missing frameEnd' }) };
        }

        // match settings
        const matchSettingsQuery = `
            SELECT
                slippi_version as replayFormatVersion,
                match_id as startTimeStamp,
                stage as stageId,
                timer as timerStart,
                frame_count as frameCount
            FROM match_settings
            WHERE match_id = '${ matchId }'
        `;

        const [ matchSettingsResults ] = await runAthenaQuery(matchSettingsQuery);

        const matchSettings = {
            ...matchSettingsResults,
            frameCount: Number(matchSettingsResults.frameCount),
            stage: Number(matchSettingsResults.stage),
            timer: Number(matchSettingsResults.timer),
            ...matchSettingsDefaults
        };

        // player settings
        const playerSettingsQuery = `
            SELECT
                player_index as playerIndex,
                port,
                ext_char as externalCharacterId,
                player_type as playerType,
                player_tag as nametag,
                player_tag as displayName,
                slippi_code as connectCode
            FROM player_settings
            WHERE match_id = '${ matchId }'
        `;
        const playerSettingsResult = await runAthenaQuery(playerSettingsQuery);

        const playerSettings = playerSettingsResult.map( player => {
            return {
                ...player,
                playerIndex: Number(player.playerIndex),
                port: Number(player.port),
                externalCharacterId: Number(player.externalCharacterId),
                playerType: Number(player.playerType),
                ...playerSettingsDefaults
            }
        });

        // frames
        const framesQuery = `
            SELECT *
            FROM frames
            WHERE match_id = '${ matchId }'
              AND frame_number BETWEEN ${frameStart} AND ${frameEnd}
            ORDER BY frame_number ASC
        `;
        const framesResult = await runAthenaQuery(framesQuery);

        const itemsQuery = `
            SELECT
                frame as frameNumber,
                item_type as typeId,
                state,
                face_dir as facingDirection,
                xvel as xVelocity,
                yvel as yVelocity,
                xpos as xPosition,
                ypos as yPosition,
                spawn_id as spawnId,
                missile_type as samusMissileType,
                turnip_face as peachTurnipFace,
                is_launched as isChargeShotLaunched,
                charged_power as chargeShotLevel,
                owner
            FROM items
            WHERE match_id = '${matchId}'
              AND frame BETWEEN ${frameStart} AND ${frameEnd}
        `;
        const itemFrames = await runAthenaQuery(itemsQuery);

        const platformsQuery = `
            SELECT *
            FROM (
                     -- Platform changes within the range
                     SELECT *
                     FROM platforms
                     WHERE match_id = '${matchId}'
                       AND frame BETWEEN ${frameStart} AND ${frameEnd}

                     UNION ALL

                     -- Most recent right platform change before the range
                     SELECT *
                     FROM (
                              SELECT *
                              FROM platforms
                              WHERE match_id = '${matchId}'
                                AND platform = 0
                                AND frame < ${frameStart}
                              ORDER BY frame DESC
                                  LIMIT 1
                          )

                     UNION ALL

                     -- Most recent left platform change before the range
                     SELECT *
                     FROM (
                              SELECT *
                              FROM platforms
                              WHERE match_id = '${matchId}'
                                AND platform = 1
                                AND frame < ${frameStart}
                              ORDER BY frame DESC
                                  LIMIT 1
                          )
                 ) t
            ORDER BY frame;
        `;
        const platformFrames = await runAthenaQuery(platformsQuery);

        const groupedFrames = new Map();

        for (const frame of framesResult) {
            const f = frame.frame_number;
            if (!groupedFrames.has(f)) {
                groupedFrames.set(f, []);
            }
            groupedFrames.get(f).push(frame);
        }

        let frames = [];

        for (const [frameNumber, frameGroup] of groupedFrames.entries()) {
            const players = [];
            const items = [];

            for (const frame of frameGroup) {
                players.push({
                    frameNumber: Number(frame.frame_number),
                    playerIndex: Number(frame.player_index),
                    inputs: {
                        frameNumber: Number(frame.frame_number),
                        playerIndex: Number(frame.player_index),
                        isNana: frame.follower === 'true',
                        physical: {
                            dPadLeft: Boolean(frame.buttons & 0x0001),
                            dPadRight: Boolean(frame.buttons & 0x0002),
                            dPadDown: Boolean(frame.buttons & 0x0004),
                            dPadUp: Boolean(frame.buttons & 0x0008),
                            z: Boolean(frame.buttons & 0x0010),
                            rTriggerAnalog: Number(frame.phys_r),
                            rTriggerDigital: Boolean(frame.buttons & 0x0020),
                            lTriggerAnalog: Number(frame.phys_l),
                            lTriggerDigital: Boolean(frame.buttons & 0x0040),
                            a: Boolean(frame.buttons & 0x0100),
                            b: Boolean(frame.buttons & 0x0200),
                            x: Boolean(frame.buttons & 0x0400),
                            y: Boolean(frame.buttons & 0x0800),
                            start: Boolean(frame.buttons & 0x1000),
                        },
                        processed: {
                            dPadLeft: Boolean(frame.buttons & 0x0001),
                            dPadRight: Boolean(frame.buttons & 0x0002),
                            dPadDown: Boolean(frame.buttons & 0x0004),
                            dPadUp: Boolean(frame.buttons & 0x0008),
                            z: Boolean(frame.buttons & 0x0010),
                            rTriggerDigital: Boolean(frame.buttons & 0x0020),
                            lTriggerDigital: Boolean(frame.buttons & 0x0040),
                            a: Boolean(frame.buttons & 0x0100),
                            b: Boolean(frame.buttons & 0x0200),
                            x: Boolean(frame.buttons & 0x0400),
                            y: Boolean(frame.buttons & 0x0800),
                            start: Boolean(frame.buttons & 0x1000),
                            joystickX: Number(frame.joy_x),
                            joystickY: Number(frame.joy_y),
                            cStickX: Number(frame.c_x),
                            cStickY: Number(frame.c_y),
                            anyTrigger: Math.max(frame.phys_l, frame.phys_r)
                        }
                    },
                    state: {
                        frameNumber: Number(frame.frame_number),
                        playerIndex: Number(frame.player_index),
                        isNana: frame.follower === 'true',
                        internalCharacterId: Number(frame.char_id),
                        actionStateId: Number(frame.action_post),
                        xPosition: Number(frame.pos_x_post),
                        yPosition: Number(frame.pos_y_post),
                        facingDirection: Number(frame.face_dir_post),
                        percent: Number(frame.percent_post),
                        shieldSize: Number(frame.shield),
                        lastHittingAttackId: Number(frame.hit_with),
                        currentComboCount: Number(frame.combo),
                        lastHitBy: Number(frame.hurt_by),
                        stocksRemaining: Number(frame.stocks),
                        actionStateFrameCounter: Number(frame.action_fc),
                        hitstunRemaining: Number(frame.hitstun),
                        isGrounded: !frame.airborne,
                        lastGroundId: Number(frame.ground_id),
                        jumpsRemaining: Number(frame.jumps),
                        lCancelStatus: Number(frame.l_cancel),
                        hurtboxCollisionState: Number(frame.hurtbox),
                        selfInducedAirXSpeed: Number(frame.self_air_x),
                        selfInducedAirYSpeed: Number(frame.self_air_y),
                        attackBasedXSpeed: Number(frame.attack_x),
                        attackBasedYSpeed: Number(frame.attack_y),
                        selfInducedGroundXSpeed: Number(frame.self_grd_x),
                        hitlagRemaining: Number(frame.hitlag),
                        isInHitstun: frame.hitstun > 0,
                        isDead: !frame.alive,
                        ...playerStateDefaults
                    }
                });
            }

            // Items for the frame
            let relevantItemFrames = itemFrames.filter(itemFrame => itemFrame.frameNumber === frameNumber);
            for (const itemFrame of relevantItemFrames) {
                items.push({
                    matchId: itemFrame.matchId,
                    frameNumber: Number(itemFrame.frameNumber),
                    typeId: Number(itemFrame.typeId),
                    state: Number(itemFrame.state),
                    facingDirection: Number(itemFrame.facingDirection),
                    xVelocity: Number(itemFrame.xVelocity),
                    yVelocity: Number(itemFrame.yVelocity),
                    xPosition: Number(itemFrame.xPosition),
                    yPosition: Number(itemFrame.yPosition),
                    spawnId: Number(itemFrame.spawnId),
                    samusMissileType: Number(itemFrame.samusMissileType),
                    peachTurnipFace: Number(itemFrame.peachTurnipFace),
                    isChargedShotLaunched: Number(itemFrame.isChargedShotLaunched),
                    chargeShotLevel: Number(itemFrame.chargeShotLevel),
                    owner: Number(itemFrame.owner),
                    ...itemStateDefaults,
                });
            }

            // Stage state
            const stageState = {
                frameNumber: Number(frameNumber),
                fodLeftPlatformHeight: Number(getPlatformHeightAtFrame(
                    platformFrames.filter(frame => frame.platform == 1).sort((a, b) => b.frame - a.frame),
                    frameNumber
                )),
                fodRightPlatformHeight: Number(getPlatformHeightAtFrame(
                    platformFrames.filter(frame => frame.platform == 0).sort((a, b) => b.frame - a.frame),
                    frameNumber
                ))
            };

            frames.push({
                frameNumber: Number(frameNumber),
                randomSeed: Number(frameGroup[0].seed), // should be the same for all players in that frame
                players,
                items,
                stage: stageState
            });
        }

        const gameEnding = {
            ...gameEndingDefaults
        }

        const replayData = {
            settings: {
                ...matchSettings,
                playerSettings
            },
            frames,
            ending: gameEnding
        }

        return {
            statusCode: 200,
            body: JSON.stringify(replayData),
            headers: {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET',
            }
        };

    } catch (err) {
        console.error('Athena query error:', err);
        return {
            statusCode: 500,
            body: JSON.stringify({ error: err.message || 'Internal error' }),
        };
    }
};
