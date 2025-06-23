import {
    AthenaClient,
    StartQueryExecutionCommand,
    GetQueryExecutionCommand,
    GetQueryResultsCommand,
} from '@aws-sdk/client-athena';

const athena = new AthenaClient({ region: 'us-east-2' });

async function runAthenaQuery(query) {

    const start = new StartQueryExecutionCommand({
        QueryString: query,
        QueryExecutionContext: { Database: process.env.GLUE_DATABASE },
        ResultConfiguration: { OutputLocation: process.env.QUERY_OUTPUT_LOCATION },
    });

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

export const handler = async (event) => {
    try {
        // const query = event.queryStringParameters?.query;
        // if (!query) {
        //     return { statusCode: 400, body: JSON.stringify({ error: 'Missing query parameter' }) };
        // }

        const matchId = '2025-06-02T04:32:24Z';

        const query = `
            SELECT
                slippi_version as replayFormatVersion,
                match_id as startTimeStamp,
                stage as stageId,
                timer as timerStart
            FROM match_settings
            WHERE match_id = '${matchId}'
        `;

        const data = await runAthenaQuery(query);
        return {
            statusCode: 200,
            body: JSON.stringify({ data }),
        };
    } catch (err) {
        console.error('Athena query error:', err);
        return {
            statusCode: 500,
            body: JSON.stringify({ error: err.message || 'Internal error' }),
        };
    }
};
