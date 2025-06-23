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
} from "./defaults";

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
        if (change.frame > currentFrame) {
            break;
        }
        height = change.platform_height;
    }
    return height;
}

export const handler = async (event) => {
    try {
        const { matchId, frameStart, frameEnd } = event;
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
                timer as timerStart
            FROM match_settings
            WHERE match_id = '${ matchId }'
        `;
        const matchSettings = {
            ...await runAthenaQuery(matchSettingsQuery),
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
                ...playerSettingsDefaults
            }
        });

        // frames
        const framesQuery = `
            SELECT *
            FROM frames
            WHERE match_id = '${ matchId }'
            AND frame_number BETWEEN ${frameStart} AND ${frameEnd}
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
            FROM platforms
            WHERE match_id = ${ matchId }
            AND frame BETWEEN ${frameStart} AND ${frameEnd}
        `;
        const platformFrames = await runAthenaQuery(platformsQuery);

        let frames = [];

        for (const frame of framesResult) {
            let players = [];
            let items = [];
            players[frame.player_index] = {
                frameNumber: frame.frame_number,
                playerIndex: frame.player_index,
                inputs: {
                    frameNumber: frame.frame_number,
                    playerIndex: frame.player_index,
                    isNana: frame.follower,
                    physical: {
                        dPadLeft: Boolean(frame.buttons & 0x0001),
                        dPadRight: Boolean(frame.buttons & 0x0002),
                        dPadDown: Boolean(frame.buttons & 0x0004),
                        dPadUp: Boolean(frame.buttons & 0x0008),
                        z: Boolean(frame.buttons & 0x0010),
                        rTriggerAnalog: frame.phys_r,
                        rTriggerDigital: Boolean(frame.buttons & 0x0020),
                        lTriggerAnalog: frame.phys_l,
                        lTriggerDigital: Boolean(frame.buttons & 0x0040),
                        a: Boolean(frame.buttons & 0x0100),
                        b: Boolean(frame.buttons & 0x0200),
                        x: Boolean(frame.buttons & 0x0400),
                        y: Boolean(frame.buttons & 0x0800),
                        start: Boolean(frame.buttons & 0x1000),
                    },
                    processed: { // TODO: slippc is a little weird about this
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
                        joystickX: frame.joy_x,
                        joystickY: frame.joy_y,
                        cStickX: frame.c_x,
                        cStickY: frame.c_y,
                        anyTrigger: Math.max(frame.phys_l, frame.phys_r)
                    },
                },
                // TODO: nanaInputs, naneState
                state: {
                    frameNumber: frame.frame_number,
                    playerIndex: frame.player_index,
                    isNana: frame.follower,
                    internalCharacterId: frame.char_id,
                    actionStateId: frame.action_post, // TODO: pre or post?
                    xPosition: frame.pos_x_post,
                    yPosition: frame.pos_y_post,
                    facingDirection: frame.face_dir_post,
                    percent: frame.percent_post,
                    shieldSize: frame.shield,
                    lastHittingAttackId: frame.hit_with,
                    currentComboCount: frame.combo,
                    lastHitBy:frame.hurt_by,
                    stocksRemaining: frame.stocks,
                    actionStateFrameCounter: frame.action_fc,
                    hitstunRemaining: frame.hitstun,
                    isGrounded: !frame.airborne,
                    lastGroundId: frame.ground_id,
                    jumpsRemaining: frame.jumps,
                    lCancelStatus: frame.l_cancel,
                    hurtboxCollisionState: frame.hurtbox,
                    selfInducedAirXSpeed: frame.self_air_x,
                    selfInducedAirYSpeed: frame.self_air_y,
                    attackBasedXSpeed: frame.attack_x,
                    attackBasedYSpeed: frame.attack_y,
                    selfInducedGroundXSpeed: frame.self_grd_x,
                    hitlagRemaining: frame.hitlag,
                    isInHitstun: frame.hitstun > 0,
                    isDead: !frame.alive,

                    ...playerStateDefaults
                }
            };

            let relevantItemFrames = itemFrames.filter( itemFrame => itemFrame.frameNumber === frame.frame_number);

            for (itemFrame of relevantItemFrames) {
                items.push({
                    ...itemFrame,
                    ...itemStateDefaults
                });
            }

            let stageState = {
                frameNumber: frame.frame_number,
                fodLeftPlatformHeight: getPlatformHeightAtFrame(
                    platformFrames.filter(frame => frame.platform === 1).sort(({frame: a}, {frame: b}) => b-a),
                    frame.frame_number
                ),
                fodRightPlatformHeight: getPlatformHeightAtFrame(
                    platformFrames.filter(frame => frame.platform === 0).sort(({frame: a}, {frame: b}) => b-a),
                    frame.frame_number
                )
            }

            frames.push({
                frameNumber: frame.frame_number,
                randomSeed: frame.seed,
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

        console.log(JSON.stringify(replayData, null, 2));
        return JSON.stringify(replayData);

    } catch (err) {
        console.error('Athena query error:', err);
        return {
            statusCode: 500,
            body: JSON.stringify({ error: err.message || 'Internal error' }),
        };
    }
};
