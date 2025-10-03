import fp from 'fastify-plugin'
import autoload from '@fastify/autoload'
import { join } from 'desm'


class Requisition {

    async getPurchaseOnProgress(server, userId) {
        let dbClient;
        try {
            dbClient = await server.pg.connect();

            const query = `
                WITH wf_activities AS (
                SELECT DISTINCT ON (wfa.record_id, au.ad_user_id)wfa.record_id, wfa.created, au.name AS user_name, au.ad_user_id
                FROM ad_wf_activity wfa
                JOIN ad_user au ON wfa.ad_user_id = au.ad_user_id
                WHERE wfa.ad_wf_node_id > 1000000 AND wfa.wfstate = 'CC' AND wfa.ad_table_id = 702 -- requisition
                ORDER BY wfa.record_id, au.ad_user_id, wfa.created ASC
                ),
                ranked_approvers AS (
                SELECT record_id, user_name, ad_user_id,
                    ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY created) AS nourut
                FROM wf_activities
                ),
                latest_process AS (
                SELECT DISTINCT ON (record_id)
                    ad_wf_process_id, record_id
                FROM ad_wf_process
                ORDER BY record_id, created DESC
                ),
                has_aborted AS (
                SELECT lp.record_id,
                    COUNT(*) FILTER (WHERE awa.wfstate = 'CA' AND awa.ad_wf_node_id > 1000000) > 0 AS aborted
                FROM latest_process lp
                JOIN ad_wf_activity awa ON awa.ad_wf_process_id = lp.ad_wf_process_id
                GROUP BY lp.record_id
                ),
                final_approvers AS (
                SELECT
                    r.record_id,
                    MAX(CASE WHEN r.nourut = 1 THEN r.user_name END) AS preparedby,
                     MAX(CASE WHEN r.nourut = 1 THEN r.ad_user_id END) AS preparedbyid,
                    MAX(CASE
                        WHEN h.aborted THEN NULL
                        WHEN r.nourut = 2 THEN r.user_name
                        END) AS legalizedby,
                    MAX(CASE
                        WHEN h.aborted THEN NULL
                        WHEN r.nourut = 2 THEN r.ad_user_id
                        END) AS legalizedbyid,
                    MAX(CASE WHEN r.nourut = 3 THEN r.user_name END) AS approvedby,
                     MAX(CASE WHEN r.nourut = 3 THEN r.ad_user_id END) AS approvedbyid
                FROM ranked_approvers r
                LEFT JOIN has_aborted h ON h.record_id = r.record_id
                GROUP BY r.record_id, h.aborted
                )
                SELECT
                mr.m_requisition_id, mr.documentno, mr.description,
                mr.datedoc, mr.docstatus, fa.preparedby, fa.legalizedby, fa.approvedby,
                fa.preparedbyid, fa.legalizedbyid, fa.approvedbyid
                FROM M_Requisition mr
                LEFT JOIN final_approvers fa ON fa.record_id = mr.m_requisition_id
                WHERE
                    mr.ad_client_id = 1000003
                    AND mr.ad_org_id = 1000003
                    AND mr.docstatus IN ('IP')
                    AND fa.preparedby IS NOT NULL
                    AND fa.legalizedby IS NOT NULL
                    AND  fa.legalizedbyid = $1`;

            const result = await dbClient.query(query, [userId]);


            return {
                success: true,
                message: 'Purchase orders workflow on progress fetched successfully',
                meta: { count: result.rowCount },
                data: result.rows.map(row => ({
                    id: row.c_order_id,
                    documentNo: row.documentno,
                    description: row.description,
                    plant: row.plant,
                    vendor: row.vendor,
                    dateOrdered: row.dateordered,
                    docStatus: row.docstatus,
                    preparedBy: row.preparedby,
                    legalizedBy: row.legalizedby,
                    approvedBy: row.approvedby
                }))
            };
        } catch (error) {
            server.log.error(error);
            return {
                success: false,
                message: 'Failed to fetch purchase orders workflow on progress',
                errors: [error.message],
                data: []
            };
        } finally {
            if (dbClient) {
                await dbClient.release();
            }
        }
    }

    async getPurchaseDone(server) {
        let dbClient;
        try {
            dbClient = await server.pg.connect();

            const query = `
                WITH wf_activities AS (
                SELECT DISTINCT ON (wfa.record_id, au.ad_user_id)wfa.record_id, wfa.created, au.name AS user_name, au.ad_user_id
                FROM ad_wf_activity wfa
                JOIN ad_user au ON wfa.ad_user_id = au.ad_user_id
                WHERE wfa.ad_wf_node_id > 1000000 AND wfa.wfstate = 'CC' AND wfa.ad_table_id = 702 -- requisition
                ORDER BY wfa.record_id, au.ad_user_id, wfa.created ASC
                ),
                ranked_approvers AS (
                SELECT record_id, user_name, ad_user_id,
                    ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY created) AS nourut
                FROM wf_activities
                ),
                latest_process AS (
                SELECT DISTINCT ON (record_id)
                    ad_wf_process_id, record_id
                FROM ad_wf_process
                ORDER BY record_id, created DESC
                ),
                has_aborted AS (
                SELECT lp.record_id,
                    COUNT(*) FILTER (WHERE awa.wfstate = 'CA' AND awa.ad_wf_node_id > 1000000) > 0 AS aborted
                FROM latest_process lp
                JOIN ad_wf_activity awa ON awa.ad_wf_process_id = lp.ad_wf_process_id
                GROUP BY lp.record_id
                ),
                final_approvers AS (
                SELECT
                    r.record_id,
                    MAX(CASE WHEN r.nourut = 1 THEN r.user_name END) AS preparedby,
                     MAX(CASE WHEN r.nourut = 1 THEN r.ad_user_id END) AS preparedbyid,
                    MAX(CASE
                        WHEN h.aborted THEN NULL
                        WHEN r.nourut = 2 THEN r.user_name
                        END) AS legalizedby,
                    MAX(CASE
                        WHEN h.aborted THEN NULL
                        WHEN r.nourut = 2 THEN r.ad_user_id
                        END) AS legalizedbyid,
                    MAX(CASE WHEN r.nourut = 3 THEN r.user_name END) AS approvedby,
                     MAX(CASE WHEN r.nourut = 3 THEN r.ad_user_id END) AS approvedbyid
                FROM ranked_approvers r
                LEFT JOIN has_aborted h ON h.record_id = r.record_id
                GROUP BY r.record_id, h.aborted
                )
                SELECT
                mr.m_requisition_id, mr.documentno, mr.description,
                mr.datedoc, mr.docstatus, fa.preparedby, fa.legalizedby, fa.approvedby,
                fa.preparedbyid, fa.legalizedbyid, fa.approvedbyid
                FROM M_Requisition mr
                LEFT JOIN final_approvers fa ON fa.record_id = mr.m_requisition_id
                WHERE
                    mr.ad_client_id = 1000003
                    AND mr.ad_org_id = 1000003
                    AND mr.docstatus IN ('CO')
                    AND fa.preparedby IS NOT NULL
                    AND fa.legalizedby IS NOT NULL
                    AND  fa.approvedby IS NOT NULL`;

            const result = await dbClient.query(query);


            return {
                success: true,
                message: 'Purchase orders workflow on progress fetched successfully',
                meta: { count: result.rowCount },
                data: result.rows.map(row => ({
                    record_id: row.c_requisition_id,
                    documentNo: row.documentno,
                    description: row.description,
                    plant: row.plant,
                    vendor: row.vendor,
                    dateOrdered: row.dateordered,
                    docStatus: row.docstatus,
                    preparedBy: row.preparedby,
                    legalizedBy: row.legalizedby,
                    approvedBy: row.approvedby
                }))
            };
        } catch (error) {
            server.log.error(error);
            return {
                success: false,
                message: 'Failed to fetch purchase orders workflow on progress',
                errors: [error.message],
                data: []
            };
        } finally {
            if (dbClient) {
                await dbClient.release();
            }
        }
    }

    async getPurchaseRevHistories(server, userId, page, pageSize) {
        let dbClient;
        try {
            dbClient = await server.pg.connect();

            const offset = (page - 1) * pageSize;

            const query = `
                SELECT
                    wfa.record_id,
                    wfa.created, mr.documentno,
                    regexp_replace(wfa.textmsg, '^IsApproved=N - ', '') AS textmsg
                FROM AD_WF_Activity wfa
                JOIN m_requisition mr ON mr.m_requisition_id = wfa.record_id
                WHERE
                    wfa.ad_table_id=702 AND wfa.wfstate = 'CA'
                    AND wfa.ad_user_id = $1
                ORDER BY wfa.created DESC
                LIMIT $2 OFFSET $3`;

            const totalCountQuery = `
                SELECT
                        COUNT(*)
                    FROM AD_WF_Activity wfa
                    JOIN m_requisition mr ON mr.m_requisition_id = wfa.record_id
                    WHERE
                        wfa.ad_table_id=702 AND wfa.wfstate = 'CA'
                        AND wfa.ad_user_id = $1
            `;

            const [result, totalCountResult] = await Promise.all([
                dbClient.query(query, [userId, pageSize, offset]),
                dbClient.query(totalCountQuery, [userId])
            ]);

            const totalCount = parseInt(totalCountResult.rows[0].count, 10);

            if (result.rows.length === 0) {
                return {
                    success: false,
                    message: `Rev History not found`,
                    meta: { count: 0 },
                    data: []
                };

            }

            return {
                success: true,
                message: 'Rev History fetched successfully',
                meta: {
                    total: totalCount,
                    count: result.rowCount,
                    per_page: pageSize,
                    current_page: page,
                    total_pages: Math.ceil(totalCount / pageSize)
                },
                data: result.rows
            }
        } catch (error) {
            console.error('Error in Rev History:', error);
            return [];
        } finally {
            if (dbClient) {
                await dbClient.release();
            }
        }
    }
}

async function requisition(fastify, opts) {
    fastify.decorate('requisition', new Requisition())
    fastify.register(autoload, {
        dir: join(import.meta.url, 'routes'),
        options: {
            prefix: opts.prefix
        }
    })
}

export default fp(requisition)