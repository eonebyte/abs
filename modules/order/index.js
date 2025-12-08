import oracleDB from '../../configs/dbOracle.js';
import fp from 'fastify-plugin'
import autoload from '@fastify/autoload'
import { join } from 'desm'
import axios from 'axios';
import https from 'https';
import fastifyJwt from '@fastify/jwt';


const baseUrlIdempiere = process.env.BASE_URL_IDEMPIERE;

class Order {

    async loadBomGraph(connection) {
        const sql = `
      SELECT 
        mpb.M_PRODUCT_ID       AS PARENT_ID,
        mpb.M_PRODUCTBOM_ID    AS CHILD_ID,
        mpb.BOMQTY             AS BOMQTY,
        CASE WHEN mp2.M_PRODUCT_CATEGORY_ID = 1000002 THEN 1 ELSE 0 END AS IS_RM
      FROM M_PRODUCT_BOM mpb
      JOIN M_PRODUCT mp2 ON mp2.M_PRODUCT_ID = mpb.M_PRODUCTBOM_ID
    `;
        const res = await connection.execute(sql);
        const graph = new Map();
        for (const row of res.rows) {
            const [parentId, childId, bomQty, isRmNum] = row;
            let arr = graph.get(parentId);
            if (!arr) {
                arr = [];
                graph.set(parentId, arr);
            }
            arr.push({ childId, bomQty, isRM: isRmNum === 1 });
        }
        return graph;
    }

    buildRmRequirements(productId, graph, memo, parentQty = 1) {
        // if (memo.has(productId)) return memo.get(productId).map(r => ({ ...r, cumulativeQty: r.cumulativeQty * parentQty }));

        const children = graph.get(productId);
        if (!children || children.length === 0) {
            memo.set(productId, []);
            return [];
        }

        const required = [];
        for (const { childId, bomQty, isRM } of children) {
            if (isRM) {
                required.push({ rmId: childId, cumulativeQty: bomQty * parentQty });
            } else {
                const subReqs = this.buildRmRequirements(childId, graph, memo, bomQty * parentQty);
                for (const sub of subReqs) {
                    required.push(sub);
                }
            }
        }
        // memo.set(productId, required.map(r => ({ ...r, cumulativeQty: r.cumulativeQty / parentQty }))); // cache normalized
        return required;
    }

    async loadDemandData(connection, startDate, endDate) {
        const sql = `
        WITH CombinedData AS (
            SELECT 'F' AS SRC, ol.M_Product_ID, SUM(ol.QtyOrdered) AS QTY, 0 AS QTYNG
            FROM C_Order c
            JOIN C_OrderLine ol ON ol.C_Order_ID = c.C_Order_ID
            JOIN M_Product p ON ol.M_Product_ID = p.M_Product_ID
            WHERE c.DocStatus = 'CO'
                AND c.IsSoTrx = 'Y'
                AND c.C_DOCTYPETARGET_ID IN (1000026, 1000115) -- FO, FPO
                AND c.DateOrdered >= TO_DATE(:startDate, 'YYYY-MM-DD')
                AND c.DateOrdered <  TO_DATE(:endDate, 'YYYY-MM-DD')
                AND p.M_Product_Category_ID IN (1000000, 1000034)
            GROUP BY ol.M_Product_ID
            HAVING SUM(ol.QtyOrdered) > 0
            UNION ALL
            SELECT 'S', ol.M_Product_ID, SUM(ol.QtyOrdered) AS QTY, 0 AS QTYNG
            FROM C_Order c
            JOIN C_OrderLine ol ON ol.C_Order_ID = c.C_Order_ID
            JOIN M_Product p ON ol.M_Product_ID = p.M_Product_ID
            WHERE c.DocStatus = 'CO'
                AND c.IsSoTrx = 'Y'
                AND c.C_DOCTYPETARGET_ID IN (1000030, 1000053, 1000054) -- Sales
                AND c.DateOrdered >= TO_DATE(:startDate, 'YYYY-MM-DD')
                AND c.DateOrdered <  TO_DATE(:endDate, 'YYYY-MM-DD')
                AND p.M_Product_Category_ID IN (1000000, 1000034)
            GROUP BY ol.M_Product_ID
            HAVING SUM(ol.QtyOrdered) > 0
            UNION ALL
            SELECT 'P', pp.M_Product_ID, SUM(pp.QTY_OK) AS QTY, SUM(pp.QTY_NG) AS QTYNG
            FROM M_Production m
            JOIN M_ProductionPlan pp ON pp.M_Production_ID = m.M_Production_ID
            JOIN M_Product p ON pp.M_Product_ID = p.M_Product_ID
            WHERE pp.Processed = 'Y'
                AND m.MovementDate >= TO_DATE(:startDate, 'YYYY-MM-DD')
                AND m.MovementDate <  TO_DATE(:endDate, 'YYYY-MM-DD')
                AND p.M_Product_Category_ID IN (1000000, 1000034)
            GROUP BY pp.M_Product_ID
            HAVING SUM(pp.QTY_OK) > 0
            UNION ALL
            SELECT 'D', iol.M_Product_ID, SUM(iol.MOVEMENTQTY) AS QTY, 0 AS QTYNG
            FROM M_InOut io
            JOIN M_InOutLine iol ON iol.M_INOUT_ID = io.M_INOUT_ID
            JOIN M_Product p ON iol.M_Product_ID = p.M_Product_ID
            WHERE io.ISSOTRX = 'Y'
                AND io.ADW_TMS_ID IS NOT NULL
                AND io.DateOrdered >= TO_DATE(:startDate, 'YYYY-MM-DD')
                AND io.DateOrdered <  TO_DATE(:endDate, 'YYYY-MM-DD')
                AND p.M_Product_Category_ID IN (1000000, 1000034)
                AND io.M_WAREHOUSE_ID = 1000011 -- Wh Delivery Preparation
            GROUP BY iol.M_Product_ID
            HAVING SUM(iol.MOVEMENTQTY) > 0
            UNION ALL
            SELECT 'SP', mvl.M_Product_ID, SUM(mvl.MOVEMENTQTY) AS QTY, 0 AS QTYNG
            FROM M_Movement mv
            JOIN M_MovementLine mvl ON mvl.M_MOVEMENT_ID = mv.M_MOVEMENT_ID
            JOIN M_Product p ON mvl.M_Product_ID = p.M_Product_ID
            WHERE mv.PROCESSED = 'Y'
                AND mv.MovementDate >= TO_DATE(:startDate, 'YYYY-MM-DD')
                AND mv.MovementDate < TO_DATE(:endDate, 'YYYY-MM-DD')
                AND p.M_Product_Category_ID = 1000002
                AND mv.M_WAREHOUSE_ID = 1000002 -- Wh RM
                AND mv.M_WAREHOUSETO_ID = 1000007 -- Wh Produksi
            GROUP BY mvl.M_Product_ID
            HAVING SUM(mvl.MOVEMENTQTY) > 0
            UNION ALL
            SELECT 'RP', mvl.M_Product_ID, SUM(mvl.MOVEMENTQTY) AS QTY, 0 AS QTYNG
            FROM M_Movement mv
            JOIN M_MovementLine mvl ON mvl.M_MOVEMENT_ID = mv.M_MOVEMENT_ID
            JOIN M_Product p ON mvl.M_Product_ID = p.M_Product_ID
            WHERE mv.PROCESSED = 'Y'
                AND mv.MovementDate >= TO_DATE(:startDate, 'YYYY-MM-DD')
                AND mv.MovementDate < TO_DATE(:endDate, 'YYYY-MM-DD')
                AND p.M_Product_Category_ID = 1000002
                AND mv.M_WAREHOUSE_ID = 1000007 -- Wh Produksi
                AND mv.M_WAREHOUSETO_ID = 1000002 -- Wh Rm
            GROUP BY mvl.M_Product_ID
            HAVING SUM(mvl.MOVEMENTQTY) > 0
        )
        SELECT cd.SRC, cd.M_Product_ID, mp.VALUE, mp.NAME, cd.QTY, cd.QTYNG
        FROM CombinedData cd
        JOIN M_PRODUCT mp ON cd.M_Product_ID = mp.M_PRODUCT_ID
    `;
        return connection.execute(sql, { startDate, endDate });
    }

    async loadProductDetails(connection, productIds) {
        if (productIds.length === 0) return new Map();

        const bindNames = productIds.map((_, i) => `:id${i}`);
        const sql = `
        SELECT M_PRODUCT_ID, VALUE, NAME
        FROM M_PRODUCT
        WHERE M_PRODUCT_ID IN (${bindNames.join(',')})
        `;
        const binds = {};
        productIds.forEach((id, i) => { binds[`id${i}`] = id; });

        const res = await connection.execute(sql, binds);
        const m = new Map();
        for (const row of res.rows) {
            const [id, val, name] = row;
            m.set(id, { rmKey: val, rmName: name });
        }
        return m;
    }

    async calculateRmRequirements(startDateStr, endDateStr) {
        // Siapkan Date JS untuk bind
        const startDate = startDateStr;
        const endDate = endDateStr;

        const round2 = (n) => Math.round((n + Number.EPSILON) * 100) / 100;

        let connection;
        try {
            connection = await oracleDB.openConnection();  // pastikan ini ambil dari pool

            // Jalankan demand & BOM secara paralel
            const [demandRes, bomGraph] = await Promise.all([
                this.loadDemandData(connection, startDate, endDate),
                this.loadBomGraph(connection)
            ]);

            const demandRows = demandRes.rows;
            if (demandRows.length === 0) return [];

            const memo = new Map();  // explosion cache
            const rmAggregator = new Map(); // Map<rmId, {ListFG:Map<fgId,Obj>, supplyProductionQtyKg?:number}>

            for (const row of demandRows) {
                const [src, productId, productKey, productName, productQty, productQtyNg] = row;

                if (src === 'SP') {
                    if (!rmAggregator.has(productId)) {
                        rmAggregator.set(productId, { ListFG: new Map(), supplyProductionQtyKg: 0 });
                    }
                    rmAggregator.get(productId).supplyProductionQtyKg += productQty;
                    continue;
                } else if (src === 'RP') {
                    if (!rmAggregator.has(productId)) {
                        rmAggregator.set(productId, { ListFG: new Map(), returnProductionQtyKg: 0 });
                    }
                    rmAggregator.get(productId).returnProductionQtyKg += productQty;
                    continue;
                }

                // explode BOM in memory
                const rmReqs = this.buildRmRequirements(productId, bomGraph, memo, productQty);

                for (const req of rmReqs) {
                    if (!rmAggregator.has(req.rmId)) {
                        rmAggregator.set(req.rmId, { ListFG: new Map(), supplyProductionQtyKg: 0, returnProductionQtyKg: 0 });
                    }
                    const agg = rmAggregator.get(req.rmId);
                    if (!agg.ListFG.has(productId)) {
                        agg.ListFG.set(productId, {
                            fgKey: productKey,
                            fgName: productName,
                            forecastOrderQtyPcs: 0,
                            forecastOrderQtyKg: 0,
                            salesOrderQtyPcs: 0,
                            salesOrderQtyKg: 0,
                            supplyProductionQtyKg: 0,
                            productionQtyOkPcs: 0,
                            productionQtyOkKg: 0,
                            productionQtyNgKg: 0,
                            deliveryQtyPcs: 0,
                            deliveryQtyKg: 0,
                        });
                    }
                    const fgUsage = agg.ListFG.get(productId);
                    switch (src) {
                        case 'F':
                            fgUsage.forecastOrderQtyPcs += productQty;
                            fgUsage.forecastOrderQtyKg += req.cumulativeQty;
                            break;
                        case 'S':
                            fgUsage.salesOrderQtyPcs += productQty;
                            fgUsage.salesOrderQtyKg += req.cumulativeQty;
                            break;
                        case 'P':
                            fgUsage.productionQtyOkPcs += productQty;
                            fgUsage.productionQtyOkKg += req.cumulativeQty;
                            if (productQty > 0) {
                                fgUsage.productionQtyNgKg += productQtyNg * (req.cumulativeQty / productQty);
                            }
                            break;
                        case 'D':
                            fgUsage.deliveryQtyPcs += productQty;
                            fgUsage.deliveryQtyKg += req.cumulativeQty;
                            break;
                    }
                }
            }

            // Distribusi supplyProduction ke tiap FG yang memakai RM tsb
            for (const data of rmAggregator.values()) {
                for (const fg of data.ListFG.values()) {
                    fg.supplyProductionQtyKg += data.supplyProductionQtyKg || 0;
                    fg.returnProductionQtyKg += data.returnProductionQtyKg || 0;
                }
            }

            for (const row of demandRows) {
                const [src, productId, productKey, productName, productQty, productQtyNg] = row;
                if (src === 'SP' && productKey === '1036285') {
                    console.log('SP row debug:', { productId, productKey, productQty });
                }
            }


            // Ambil detail rmKey/rmName
            const allRmIds = Array.from(rmAggregator.keys());
            const rmDetailsMap = await this.loadProductDetails(connection, allRmIds);

            // Bentuk final
            const finalReport = [];
            for (const [rmId, data] of rmAggregator.entries()) {
                const rmDetails = rmDetailsMap.get(rmId);
                if (!rmDetails) continue;
                finalReport.push({
                    ...rmDetails,
                    supplyProductionQtyKg: round2(data.supplyProductionQtyKg),
                    returnProductionQtyKg: round2(data.returnProductionQtyKg),
                    ListFG: Array.from(data.ListFG.values())
                        .map(fg => ({
                            ...fg,
                            forecastOrderQtyKg: round2(fg.forecastOrderQtyKg),
                            salesOrderQtyKg: round2(fg.salesOrderQtyKg),
                            productionQtyOkKg: round2(fg.productionQtyOkKg),
                            productionQtyNgKg: round2(fg.productionQtyNgKg),
                            deliveryQtyKg: round2(fg.deliveryQtyKg)
                        }))
                        .sort((a, b) => a.fgKey.localeCompare(b.fgKey))
                });
            }

            return finalReport.sort((a, b) => a.rmKey.localeCompare(b.rmKey));
        } finally {
            if (connection) await connection.close().catch(err => console.error('close conn fail', err));
        }
    }

    async getPurchaseSuspend(server) {
        let dbClient;
        try {
            dbClient = await server.pg.connect();

            const query = `
                WITH wf_activities AS (
                SELECT DISTINCT ON (wfa.record_id, au.ad_user_id)wfa.record_id, wfa.created, au.name AS user_name
                FROM ad_wf_activity wfa
                JOIN ad_user au ON wfa.ad_user_id = au.ad_user_id
                WHERE wfa.ad_wf_node_id > 1000000 AND wfa.wfstate = 'CC'
                ORDER BY wfa.record_id, au.ad_user_id, wfa.created ASC
                ),
                ranked_approvers AS (
                SELECT record_id, user_name,
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
                    MAX(CASE
                        WHEN h.aborted THEN NULL
                        WHEN r.nourut = 2 THEN r.user_name
                        END) AS legalizedby,
                    MAX(CASE WHEN r.nourut = 3 THEN r.user_name END) AS approvedby
                FROM ranked_approvers r
                LEFT JOIN has_aborted h ON h.record_id = r.record_id
                GROUP BY r.record_id, h.aborted
                )
                SELECT
                co.c_order_id, co.documentno, co.description, cbl."name" AS plant, cb."name" AS vendor,
                co.dateordered, co.docstatus, fa.preparedby, fa.legalizedby, fa.approvedby
                FROM C_Order co
                JOIN c_bpartner cb ON cb.c_bpartner_id = co.c_bpartner_id
                JOIN c_bpartner_location cbl ON co.c_bpartner_location_id = cbl.c_bpartner_location_id
                LEFT JOIN final_approvers fa ON fa.record_id = co.c_order_id
                WHERE
                    co.ad_client_id = 1000003
                    AND co.ad_org_id = 1000003
                    AND co.docstatus IN ('IP')
                    AND (fa.legalizedby IS NULL OR fa.approvedby IS NULL)`;

            const result = await dbClient.query(query);


            if (result.rows && result.rows.length > 0) {
                return result.rows.map(row => ({
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
                }));
            } else {
                return [];
            }
        } catch (error) {
            console.error('Error in getPurchaseOrders:', error);
            return [];
        } finally {
            if (dbClient) {
                await dbClient.release();
            }
        }
    }

    async getPurchaseApproved(server) {
        let dbClient;
        try {
            dbClient = await server.pg.connect();

            const query = `
                WITH wf_activities AS (
                SELECT DISTINCT ON (wfa.record_id, au.ad_user_id)wfa.record_id, wfa.created, au.name AS user_name
                FROM ad_wf_activity wfa
                JOIN ad_user au ON wfa.ad_user_id = au.ad_user_id
                WHERE wfa.ad_wf_node_id > 1000000 AND wfa.wfstate = 'CC'
                ORDER BY wfa.record_id, au.ad_user_id, wfa.created ASC
                ),
                ranked_approvers AS (
                SELECT record_id, user_name,
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
                    MAX(CASE
                        WHEN h.aborted THEN NULL
                        WHEN r.nourut = 2 THEN r.user_name
                        END) AS legalizedby,
                    MAX(CASE WHEN r.nourut = 3 THEN r.user_name END) AS approvedby
                FROM ranked_approvers r
                LEFT JOIN has_aborted h ON h.record_id = r.record_id
                GROUP BY r.record_id, h.aborted
                )
                SELECT
                co.c_order_id, co.documentno, co.description, cbl."name" AS plant, cb."name" AS vendor,
                co.dateordered, co.docstatus, fa.preparedby, fa.legalizedby, fa.approvedby
                FROM C_Order co
                JOIN c_bpartner cb ON cb.c_bpartner_id = co.c_bpartner_id
                JOIN c_bpartner_location cbl ON co.c_bpartner_location_id = cbl.c_bpartner_location_id
                LEFT JOIN final_approvers fa ON fa.record_id = co.c_order_id
                WHERE
                    co.ad_client_id = 1000003
                    AND co.ad_org_id = 1000003
                    AND co.docstatus IN ('CO')
                    AND fa.preparedby IS NOT NULL
                    AND fa.legalizedby IS NOT NULL
                    AND fa.approvedby IS NOT NULL`;

            const result = await dbClient.query(query);


            if (result.rows && result.rows.length > 0) {
                return result.rows.map(row => ({
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
                }));
            } else {
                return [];
            }
        } catch (error) {
            console.error('Error in getPurchaseOrders:', error);
            return [];
        } finally {
            if (dbClient) {
                await dbClient.release();
            }
        }
    }


    async setUserChoice(request, orderId, choiceValue) {
        const user = request.user;

        if (!user || !user.id || !user.idempiereToken) {
            throw new Error('User not authenticated');
        }

        try {
            const agent = new https.Agent({ rejectUnauthorized: false });

            // 1. Ambil semua workflow aktif dari user
            const res = await axios.get(
                `${baseUrlIdempiere}/api/v1/workflow/${user.id}`,
                {
                    headers: {
                        Authorization: `Bearer ${user.idempiereToken}`
                    },
                    httpsAgent: agent
                }
            );
            const nodes = res.data.nodes || [];
            // 2. Cari node dengan record_id yang cocok
            const matched = nodes.find(node => node.record_id == orderId);
            if (!matched) {
                console.log(`No workflow node found for order ${orderId}`);
                return { success: false, message: 'No matching workflow activity node' };
            }
            const wfActivityId = matched.id;
            // 3. Submit user choice
            const response = await axios.put(
                `${baseUrlIdempiere}/api/v1/workflow/setuserchoice/${wfActivityId}`,
                {
                    message: 'Approve with Rest API',
                    value: choiceValue
                },
                {
                    headers: {
                        Authorization: `Bearer ${user.idempiereToken}`,
                        'Content-Type': 'application/json'
                    },
                    httpsAgent: agent
                }
            );
            return {
                success: true,
                message: 'Workflow approved',
                wfActivityId,
                response: response.data
            };

        } catch (error) {
            console.error('Error setting user choice:', error.message);
            return { success: false, error: error.message };
        }
    }

    async getWorkflowActive(request) {
        const user = request.user;

        if (!user || !user.id || !user.idempiereToken) {
            throw new Error('User not authenticated');
        }

        const agent = new https.Agent({ rejectUnauthorized: false });
        const token = user.idempiereToken;

        try {
            // 1. Ambil semua workflow aktif dari user
            const wfRes = await axios.get(
                `${baseUrlIdempiere}/api/v1/workflow/${user.id}`,
                {
                    headers: { Authorization: `Bearer ${token}` },
                    httpsAgent: agent
                }
            );

            const nodes = wfRes.data?.nodes || [];

            // 2. Filter hanya node C_Order
            const orderNodes = nodes.filter(n => n['table-name'] === 'C_Order');

            // 3. Ambil detail C_Order dari masing-masing record_id
            const orderDetails = await Promise.all(orderNodes.map(async (node) => {
                try {
                    const orderRes = await axios.get(
                        `${baseUrl}/api/v1/models/C_Order/${node.record_id}`,
                        {
                            headers: { Authorization: `Bearer ${token}` },
                            httpsAgent: agent
                        }
                    );

                    return {
                        ...node,
                        order: orderRes.data
                    };
                } catch (err) {
                    console.error(`Failed to fetch order ${node.record_id}`, err.message);
                    return {
                        ...node,
                        order: null,
                        error: `Gagal ambil data order ${node.record_id}`
                    };
                }
            }));

            return {
                success: true,
                message: 'Workflow aktif berhasil diambil',
                rowCount: orderDetails.length,
                data: orderDetails
            };

        } catch (error) {
            console.error('Error fetching workflow active:', error.message);
            return { success: false, error: error.message };
        }
    }

    async getPurchasePrint(tableName, recordId) {
        const user = request.session.get('user');
        if (!user || !user.id || !user.token) { throw new Error('User not authenticated'); }

        try {
            const response = await axios.get(`${baseUrlIdempiere}/api/models/${tableName}/${recordId}/print`, {
                responseType: 'arraybuffer', // karena PDF
                headers: {
                    Authorization: `Bearer ${token}`,
                    Accept: 'application/pdf',
                },
            });

            return {
                fileName: `${tableName}_${recordId}.pdf`,
                fileBuffer: Buffer.from(response.data, 'binary'),
            };
        } catch (err) {
            console.error('Error fetching print:', err.message);
            throw new Error('Gagal mengambil dokumen print dari iDempiere');
        }
    }

    async getPurchaseOnProgress(server, userId, page, pageSize) {
        let dbClient;
        try {
            dbClient = await server.pg.connect();

            const offset = (page - 1) * pageSize;

            const query = `
                WITH wf_activities AS (
                SELECT DISTINCT ON (wfa.record_id, au.ad_user_id)wfa.record_id, wfa.created, au.name AS user_name, au.ad_user_id
                FROM ad_wf_activity wfa
                JOIN ad_user au ON wfa.ad_user_id = au.ad_user_id
                WHERE wfa.ad_wf_node_id > 1000000 AND wfa.wfstate = 'CC' AND wfa.ad_table_id = 259 -- purchase order
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
                co.c_order_id, co.documentno, co.description, cbl."name" AS plant, cb."name" AS vendor,
                co.dateordered, co.docstatus, fa.preparedby, fa.legalizedby, fa.approvedby,
                fa.preparedbyid, fa.legalizedbyid, fa.approvedbyid
                FROM C_Order co
                JOIN c_bpartner cb ON cb.c_bpartner_id = co.c_bpartner_id
                JOIN c_bpartner_location cbl ON co.c_bpartner_location_id = cbl.c_bpartner_location_id
                LEFT JOIN final_approvers fa ON fa.record_id = co.c_order_id
                WHERE
                    co.ad_client_id = 1000003
                    AND co.ad_org_id = 1000003
                    AND co.docstatus IN ('IP')
                    AND fa.preparedby IS NOT NULL
                    AND fa.legalizedby IS NOT NULL
                    AND  fa.legalizedbyid = $1
                ORDER BY co.dateordered DESC, co.c_order_id DESC
                LIMIT $2 OFFSET $3`;

            const totalCountQuery = `
            WITH wf_activities AS (
                SELECT DISTINCT ON (wfa.record_id, au.ad_user_id)wfa.record_id, wfa.created, au.name AS user_name, au.ad_user_id
                FROM ad_wf_activity wfa
                JOIN ad_user au ON wfa.ad_user_id = au.ad_user_id
                WHERE wfa.ad_wf_node_id > 1000000 AND wfa.wfstate = 'CC' AND wfa.ad_table_id = 259 -- purchase order
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
                    COUNT(*)
                FROM C_Order co
                JOIN c_bpartner cb ON cb.c_bpartner_id = co.c_bpartner_id
                JOIN c_bpartner_location cbl ON co.c_bpartner_location_id = cbl.c_bpartner_location_id
                LEFT JOIN final_approvers fa ON fa.record_id = co.c_order_id
                WHERE
                    co.ad_client_id = 1000003
                    AND co.ad_org_id = 1000003
                    AND co.docstatus IN ('IP')
                    AND fa.preparedby IS NOT NULL
                    AND fa.legalizedby IS NOT NULL
                    AND  fa.legalizedbyid = $1`;

            const [result, totalCountResult] = await Promise.all([
                dbClient.query(query, [userId, pageSize, offset]),
                dbClient.query(totalCountQuery, [userId])
            ]);

            const totalCount = parseInt(totalCountResult.rows[0].count, 10);


            return {
                success: true,
                message: 'Purchase orders workflow on progress fetched successfully',
                meta: {
                    total: totalCount,
                    count: result.rowCount,
                    per_page: pageSize,
                    current_page: page,
                    total_pages: Math.ceil(totalCount / pageSize)
                },
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

    async getFlowProgress(server, orderId) {
        let dbClient;
        try {
            dbClient = await server.pg.connect();

            const query = `
                WITH wf_activities AS (
                    SELECT DISTINCT ON (wfa.record_id, au.ad_user_id)
                        wfa.record_id,
                        wfa.created,
                        wfa.updated,
                        wfa.textmsg,
                        au.name AS user_name,
                        au.title,
                        wfa.ad_wf_activity_uu AS barcode
                    FROM ad_wf_activity wfa
                    JOIN ad_user au ON wfa.ad_user_id = au.ad_user_id
                    WHERE wfa.ad_wf_node_id > 1000000 AND wfa.ad_table_id=259 --order
                    AND wfa.wfstate = 'CC'
                    ORDER BY wfa.record_id, au.ad_user_id, wfa.created ASC
                ),
                ranked_approvers AS (
                    SELECT
                        record_id,
                        user_name,
                        title,
                        barcode,
                        created,
                        updated,
                        textmsg,
                        ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY created) AS nourut
                    FROM wf_activities
                ),
                latest_process AS (
                    SELECT DISTINCT ON (record_id)
                        ad_wf_process_id,
                        record_id
                    FROM ad_wf_process
                    ORDER BY record_id, created DESC
                ),
                has_aborted AS (
                    SELECT
                        lp.record_id,
                        COUNT(*) FILTER (
                            WHERE awa.wfstate = 'CA'
                            AND awa.ad_wf_node_id > 1000000
                        ) > 0 AS aborted
                    FROM latest_process lp
                    JOIN ad_wf_activity awa ON awa.ad_wf_process_id = lp.ad_wf_process_id
                    GROUP BY lp.record_id
                ),
                final_approvers AS (
                    SELECT
                        r.record_id,
                        MAX(CASE WHEN r.nourut = 1 THEN r.user_name END) AS preparedby,
                        MAX(CASE WHEN r.nourut = 1 THEN r.created END) AS preparedCreated,
                        MAX(CASE WHEN r.nourut = 1 THEN r.title END) AS preparedTitle,
                        MAX(CASE WHEN r.nourut = 1 THEN r.barcode END) AS preparedBarcode,
                        MAX(CASE WHEN r.nourut = 1 THEN r.textmsg END) AS preparedMsg,
                        MAX(CASE
                            WHEN h.aborted THEN NULL
                            WHEN r.nourut = 2 THEN r.user_name
                        END) AS legalizedby,
                         MAX(CASE
                            WHEN h.aborted THEN NULL
                            WHEN r.nourut = 2 THEN r.updated
                        END) AS legalizedCreated,
                        MAX(CASE
                            WHEN h.aborted THEN NULL
                            WHEN r.nourut = 2 THEN r.title
                        END) AS legalizedTitle,
                        MAX(CASE
                            WHEN h.aborted THEN NULL
                            WHEN r.nourut = 2 THEN r.barcode
                        END) AS legalizedBarcode,
                        MAX(CASE
                            WHEN h.aborted THEN NULL
                            WHEN r.nourut = 2 THEN r.textmsg
                        END) AS legalizedMsg,
                        MAX(CASE WHEN r.nourut = 3 THEN r.user_name END) AS approvedby,
                        MAX(CASE WHEN r.nourut = 3 THEN r.created END) AS approvedCreated,
                        MAX(CASE WHEN r.nourut = 3 THEN r.title END) AS approvedTitle,
                        MAX(CASE WHEN r.nourut = 3 THEN r.barcode END) AS approvedBarcode,
                        MAX(CASE WHEN r.nourut = 3 THEN r.textmsg END) AS approvedMsg
                    FROM ranked_approvers r
                    LEFT JOIN has_aborted h ON h.record_id = r.record_id
                    GROUP BY r.record_id, h.aborted
                )
                SELECT
                    co.c_order_id,
                    fa.preparedby,
                    fa.preparedCreated,
                    fa.legalizedby,
                    fa.legalizedCreated,
                    fa.approvedby,
                    fa.approvedCreated,
                    fa.preparedTitle,
                    fa.legalizedTitle,
                    fa.approvedTitle,
                    fa.preparedBarcode,
                    fa.legalizedBarcode,
                    fa.approvedBarcode,
                    fa.preparedMsg,
                    fa.legalizedMsg,
                    fa.approvedMsg
                FROM C_Order co
                JOIN c_bpartner cb ON cb.c_bpartner_id = co.c_bpartner_id
                JOIN c_bpartner_location cbl ON co.c_bpartner_location_id = cbl.c_bpartner_location_id
                LEFT JOIN final_approvers fa ON fa.record_id = co.c_order_id
                WHERE co.ad_client_id = 1000003
                AND co.ad_org_id = 1000003
                AND co.c_order_id = $1`;

            const result = await dbClient.query(query, [orderId]);


            return {
                success: true,
                message: 'Purchase orders workflow on progress fetched successfully',
                meta: { count: result.rowCount },
                data: result.rows.map(row => ({
                    ...row,
                    c_order_id: Number(row.c_order_id)
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

    async getPurchaseDone(server, page, pageSize) {
        let dbClient;
        try {
            dbClient = await server.pg.connect();

            const offset = (page - 1) * pageSize;

            const query = `
                WITH wf_activities AS (
                SELECT DISTINCT ON (wfa.record_id, au.ad_user_id)wfa.record_id, wfa.created, au.name AS user_name, au.ad_user_id
                FROM ad_wf_activity wfa
                JOIN ad_user au ON wfa.ad_user_id = au.ad_user_id
                WHERE wfa.ad_wf_node_id > 1000000 AND wfa.wfstate = 'CC' AND wfa.ad_table_id = 259 -- purchase order
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
                co.c_order_id, co.documentno, co.description, cbl."name" AS plant, cb."name" AS vendor,
                co.dateordered, co.docstatus, fa.preparedby, fa.legalizedby, fa.approvedby,
                fa.preparedbyid, fa.legalizedbyid, fa.approvedbyid
                FROM C_Order co
                JOIN c_bpartner cb ON cb.c_bpartner_id = co.c_bpartner_id
                JOIN c_bpartner_location cbl ON co.c_bpartner_location_id = cbl.c_bpartner_location_id
                LEFT JOIN final_approvers fa ON fa.record_id = co.c_order_id
                WHERE
                    co.ad_client_id = 1000003
                    AND co.ad_org_id = 1000003
                    AND co.docstatus IN ('CO')
                    AND fa.preparedby IS NOT NULL
                    AND fa.legalizedby IS NOT NULL
                    AND  fa.approvedbyid IS NOT NULL
                ORDER BY co.dateordered DESC, co.c_order_id DESC
                LIMIT $1 OFFSET $2`;


            const totalCountQuery = `
            WITH wf_activities AS (
                SELECT DISTINCT ON (wfa.record_id, au.ad_user_id) wfa.record_id, wfa.created, au.name AS user_name, au.ad_user_id
                FROM ad_wf_activity wfa
                JOIN ad_user au ON wfa.ad_user_id = au.ad_user_id
                WHERE wfa.ad_wf_node_id > 1000000 AND wfa.wfstate = 'CC' AND wfa.ad_table_id = 259
                ORDER BY wfa.record_id, au.ad_user_id, wfa.created ASC
            ),
            ranked_approvers AS (
                SELECT record_id, user_name, ad_user_id,
                    ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY created) AS nourut
                FROM wf_activities
            ),
            latest_process AS (
                SELECT DISTINCT ON (record_id) ad_wf_process_id, record_id
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
                    MAX(CASE WHEN h.aborted THEN NULL ELSE r.user_name END) FILTER(WHERE r.nourut = 2) AS legalizedby,
                    MAX(CASE WHEN h.aborted THEN NULL ELSE r.ad_user_id END) FILTER(WHERE r.nourut = 2) AS legalizedbyid,
                    MAX(CASE WHEN r.nourut = 3 THEN r.user_name END) AS approvedby,
                    MAX(CASE WHEN r.nourut = 3 THEN r.ad_user_id END) AS approvedbyid
                FROM ranked_approvers r
                LEFT JOIN has_aborted h ON h.record_id = r.record_id
                GROUP BY r.record_id, h.aborted
            )
            SELECT COUNT(*)
            FROM C_Order co
            JOIN c_bpartner cb ON cb.c_bpartner_id = co.c_bpartner_id
            JOIN c_bpartner_location cbl ON co.c_bpartner_location_id = cbl.c_bpartner_location_id
            LEFT JOIN final_approvers fa ON fa.record_id = co.c_order_id
            WHERE
                co.ad_client_id = 1000003
                AND co.ad_org_id = 1000003
                AND co.docstatus IN ('CO')
                AND fa.preparedby IS NOT NULL
                AND fa.legalizedby IS NOT NULL
                AND fa.approvedbyid IS NOT NULL`;

            // Menjalankan kedua query secara bersamaan untuk efisiensi
            const [result, totalCountResult] = await Promise.all([
                dbClient.query(query, [pageSize, offset]),
                dbClient.query(totalCountQuery)
            ]);

            const totalCount = parseInt(totalCountResult.rows[0].count, 10);



            return {
                success: true,
                message: 'Purchase orders workflow on progress fetched successfully',
                meta: {
                    total: totalCount,
                    count: result.rowCount,
                    per_page: pageSize,
                    current_page: page,
                    total_pages: Math.ceil(totalCount / pageSize)
                },
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

    async getPurchaseRevisionActivity(server, recordId) {
        let dbClient;
        try {
            dbClient = await server.pg.connect();

            const mainQuery = `
                SELECT 
                    ROW_NUMBER() OVER (ORDER BY wfp.updated ASC) AS norev,
                    wfp.ad_wf_process_id,
                    wfp.record_id
                FROM 
                    AD_WF_Process wfp
                WHERE 
                    wfp.ad_table_id = 259
                    AND wfp.wfstate = 'CA'
                    AND wfp.record_id = $1
                ORDER BY wfp.updated ASC
                `;
            const mainRes = await dbClient.query(mainQuery, [recordId]);

            const data = [];

            // 2. Loop tiap WF Process → ambil detail activity
            for (const row of mainRes.rows) {
                const detailQuery = `
                            SELECT 
                                CASE
                                    WHEN wfa.textmsg LIKE 'IsApproved=Y%-%' 
                                        THEN REGEXP_REPLACE(wfa.textmsg, '^IsApproved=Y - ', 'Approved: ')
                                    WHEN wfa.textmsg LIKE 'IsApproved=N%-%' 
                                        THEN REGEXP_REPLACE(wfa.textmsg, '^IsApproved=N - ', 'Not Approved: ')
                                    ELSE NULL
                                END AS status_msg,
                                wfa.updated,
                                au."name",
                                au.title
                            FROM 
                                ad_wf_activity wfa
                            JOIN ad_user au ON au.ad_user_id = wfa.updatedby
                            WHERE 
                            wfa.ad_table_id = 259
                            AND wfa.ad_wf_process_id = $1
                            AND wfa.textmsg LIKE 'IsApproved=%'
                            ORDER BY updated ASC
                    `;
                const detailRes = await dbClient.query(detailQuery, [row.ad_wf_process_id]);

                const activities = detailRes.rows
                    .filter(r => r.status_msg !== null)
                    .map(r => ({
                        msg: r.status_msg,
                        dateActivity: r.updated,
                        userName: r.name,
                        userRole: r.title
                    }));

                data.push({
                    norev: row.norev,
                    wf_process_id: row.ad_wf_process_id,
                    activity: activities
                });
            }

            return { data };

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

    async getPriceHistory(server, orderId) {
        let dbClient;
        try {
            dbClient = await server.pg.connect();

            const query = `
                SELECT DISTINCT ol.LINE, ol.AD_Client_ID, ol.AD_Org_ID, ol.C_Order_ID
                    , p.M_Product_ID, p.Value, p.Name, ol.DateOrdered, o.DocumentNo
                    , uom.UOMSYMBOL
                    , getqtyonhandwarehouse(p.M_Product_ID, o.M_Warehouse_ID) QtyOnHand
                    , w.Name Warehouse
                FROM M_Product p
                INNER JOIN C_OrderLine ol ON (ol.M_Product_ID=p.M_Product_ID)
                INNER JOIN C_Order o ON (ol.C_Order_ID=o.C_Order_ID)
                INNER JOIN C_UOM uom ON (p.C_UOM_ID=uom.C_UOM_ID)
                INNER JOIN M_Warehouse w ON (o.M_Warehouse_ID=w.M_Warehouse_ID)
                LEFT OUTER JOIN C_BPartner_Product bpp ON (p.M_Product_ID=bpp.M_Product_ID)
                WHERE ol.M_Product_ID IS NOT NULL
                AND ol.C_Order_ID = $1
                ORDER BY LINE`;

            const result = await dbClient.query(query, [orderId]);

            if (result.rows.length === 0) {
                return {
                    success: false,
                    message: `No price history found for orderId ${orderId}`,
                    meta: { count: 0 },
                    data: []
                };

            }

            return {
                success: true,
                message: 'Price history fetched successfully',
                meta: { count: result.rows.length },
                data: result.rows
            }
        } catch (error) {
            console.error('Error in getPurchaseOrders:', error);
            return [];
        } finally {
            if (dbClient) {
                await dbClient.release();
            }
        }
    }

    async getPriceHistoryDetail(server, productId, dateOrdered) {
        let dbClient;
        try {
            dbClient = await server.pg.connect();

            const query = `
                SELECT TO_CHAR(ol.DATEORDERED,'YYYY') YearOrder,TRUNC(ol.DATEORDERED,'MONTH') MonthOrder,
                        UPPER(bp.Name) Name,
                        CurrencyConvert(ol.Priceactual,o.C_Currency_ID,303,ol.DATEORDERED,114,o.AD_Client_ID, o.AD_Org_ID) Value,
                        'PO' Source, 'Price IDR' History
                FROM C_OrderLine ol
                        INNER JOIN C_Order o ON (ol.C_Order_ID=o.C_Order_ID)
                    INNER JOIN C_BPartner bp ON (o.C_BPartner_ID = bp.C_BPartner_ID)
                WHERE o.isSOTrx ='N' AND o.DocStatus IN ('CO','CL')
                    AND NOT EXISTS (SELECT 1
                                FROM C_OrderLine oll
                                        INNER JOIN C_Order oo ON (oll.C_Order_ID=oo.C_Order_ID)
                                WHERE oll.M_Product_ID = ol.M_Product_ID AND oo.C_BPartner_ID=o.C_BPartner_ID
                                        AND TO_CHAR(oll.DATEORDERED,'MON-YY') = TO_CHAR(ol.DATEORDERED,'MON-YY')
                                        AND oll.DateOrdered >= ol.DateOrdered AND oll.Created > ol.Created
                                )
                    AND ol.M_Product_ID = $1 -- Parameter M_Product_ID
                    AND ol.DateOrdered >= CAST($2 AS DATE) - INTERVAL '6 months' -- Pengganti MONTHS_BETWEEN
                    AND ol.DateOrdered <= $2 -- Parameter DateOrdered
                    AND ol.Priceactual > 0
                UNION
                SELECT TO_CHAR(ol.DATEORDERED,'YYYY') YearOrder,TRUNC(ol.DATEORDERED,'MONTH') MonthOrder,
                        UPPER(bp.Name) Name,
                        ol.Priceactual Value,
                        'PO' Source, 'Price ' || cr.ISO_Code History
                FROM C_OrderLine ol
                        INNER JOIN C_Order o ON (ol.C_Order_ID=o.C_Order_ID)
                        INNER JOIN C_Currency cr ON (o.C_Currency_ID=cr.C_Currency_ID)
                    INNER JOIN C_BPartner bp ON (o.C_BPartner_ID = bp.C_BPartner_ID)
                WHERE o.isSOTrx ='N' AND o.DocStatus IN ('CO','CL')
                    AND NOT EXISTS (SELECT 1
                                FROM C_OrderLine oll
                                        INNER JOIN C_Order oo ON (oll.C_Order_ID=oo.C_Order_ID)
                                WHERE oll.M_Product_ID = ol.M_Product_ID AND oo.C_BPartner_ID=o.C_BPartner_ID
                                        AND TO_CHAR(oll.DATEORDERED,'MON-YY') = TO_CHAR(ol.DATEORDERED,'MON-YY')
                                        AND oll.DateOrdered >= ol.DateOrdered AND oll.Created > ol.Created
                                )
                    AND ol.M_Product_ID = $1
                    AND ol.DateOrdered >= CAST($2 AS DATE) - INTERVAL '6 months'
                    AND ol.DateOrdered <= $2
                    AND ol.Priceactual > 0
                UNION
                SELECT TO_CHAR(ol.DATEORDERED,'YYYY') YearOrder,TRUNC(ol.DATEORDERED,'MONTH') MonthOrder,
                        UPPER(bp.Name) Name,
                        SUM(ol.QtyOrdered) Value,
                        'PO' Source, 'Qty PO' History
                FROM C_OrderLine ol
                        INNER JOIN C_Order o ON (ol.C_Order_ID=o.C_Order_ID)
                    INNER JOIN C_BPartner bp ON (o.C_BPartner_ID = bp.C_BPartner_ID)
                WHERE o.isSOTrx ='N' AND o.DocStatus IN ('CO','CL')
                    AND ol.M_Product_ID = $1
                    AND ol.DateOrdered >= CAST($2 AS DATE) - INTERVAL '6 months'
                    AND ol.DateOrdered <= $2
                GROUP BY TO_CHAR(ol.DATEORDERED,'YYYY'), TRUNC(ol.DATEORDERED,'MONTH'), UPPER(bp.Name)
                UNION
                SELECT TO_CHAR(io.MovementDate,'YYYY') YearOrder,TRUNC(io.MovementDate,'MONTH') MonthOrder,
                        UPPER(bp.Name) Name,
                        SUM(iol.MovementQty) Value,
                        'PO' Source, 'Qty RR' History
                FROM M_InOut io
                    INNER JOIN M_InOutLine iol ON (iol.M_InOut_ID=io.M_InOut_ID)
                    INNER JOIN C_BPartner bp ON (io.C_BPartner_ID = bp.C_BPartner_ID)
                WHERE io.isSOTrx ='N' AND io.DocStatus IN ('CO','CL')
                    AND iol.M_Product_ID = $1
                    AND io.MovementDate >= CAST($2 AS DATE) - INTERVAL '6 months'
                    AND io.MovementDate <= $2
                GROUP BY TO_CHAR(io.MovementDate,'YYYY'), TRUNC(io.MovementDate,'MONTH'), UPPER(bp.Name)
                UNION
                SELECT TO_CHAR(mpo.PriceEffective,'YYYY') YearOrder,TRUNC(mpo.PriceEffective,'MONTH') MonthOrder,
                        UPPER(bp.Name) Name,
                        CurrencyConvert(mpo.PriceList,mpo.C_Currency_ID,303,mpo.PriceEffective,114,mpo.AD_Client_ID, mpo.AD_Org_ID) Value,
                        'MPO' Source, 'Price IDR' History
                FROM M_Product_PO mpo
                    INNER JOIN C_BPartner bp ON (mpo.C_BPartner_ID = bp.C_BPartner_ID)
                WHERE mpo.IsActive='Y' And Discontinued='N'
                    AND NOT EXISTS (SELECT 1
                                FROM M_Product_PO po
                                WHERE mpo.M_Product_ID = po.M_Product_ID AND mpo.C_BPartner_ID=po.C_BPartner_ID
                                        AND TO_CHAR(mpo.PriceEffective,'MON-YY') = TO_CHAR(po.PriceEffective,'MON-YY')
                                        AND po.PriceEffective >= mpo.PriceEffective AND po.Created > mpo.Created
                                )
                    AND mpo.M_Product_ID = $1
                    AND mpo.PriceEffective >= CAST($2 AS DATE) - INTERVAL '6 months'
                    AND mpo.PriceEffective <= $2
                    AND mpo.PriceList > 0
                UNION
                (SELECT TO_CHAR(ol.DATEORDERED,'YYYY') YearOrder,TRUNC(ol.DATEORDERED,'MONTH') MonthOrder,
                        UPPER(bp.Name) Name,
                        CurrencyConvert(ol.Priceactual,o.C_Currency_ID,303,ol.DATEORDERED,114,o.AD_Client_ID, o.AD_Org_ID) Value,
                        'PO' Source, 'Price IDR' History
                FROM C_OrderLine ol
                        INNER JOIN C_Order o ON (ol.C_Order_ID=o.C_Order_ID)
                    INNER JOIN C_BPartner bp ON (o.C_BPartner_ID = bp.C_BPartner_ID)
                WHERE o.isSOTrx ='N' AND o.DocStatus IN ('CO','CL')
                    AND NOT EXISTS (SELECT 1
                                FROM C_OrderLine oll
                                        INNER JOIN C_Order oo ON (oll.C_Order_ID=oo.C_Order_ID)
                                WHERE oll.M_Product_ID = ol.M_Product_ID AND oo.C_BPartner_ID=o.C_BPartner_ID
                                        AND TO_CHAR(oll.DATEORDERED,'MON-YY') = TO_CHAR(ol.DATEORDERED,'MON-YY')
                                        AND oll.DateOrdered >= ol.DateOrdered AND oll.Created > ol.Created
                                )
                    AND ol.M_Product_ID = $1
                    AND ol.DateOrdered < CAST($2 AS DATE) - INTERVAL '6 months' -- Logic > 6 diubah menjadi <
                    AND ol.DateOrdered <= $2
                    AND ol.Priceactual > 0
                    ORDER BY ol.DateOrdered Desc
                    LIMIT 1)
                UNION
                SELECT DISTINCT TO_CHAR(ol.DATEORDERED,'YYYY') YearOrder,TRUNC(ol.DATEORDERED,'MONTH') MonthOrder,
                        UPPER(bp.Name) Name,
                        pt.NetDays Value,
                        'PO' Source, 'TOP' History
                FROM C_OrderLine ol
                        INNER JOIN C_Order o ON (ol.C_Order_ID=o.C_Order_ID)
                    INNER JOIN C_BPartner bp ON (o.C_BPartner_ID = bp.C_BPartner_ID)
                        INNER JOIN C_PaymentTerm pt ON (o.C_PaymentTerm_ID=pt.C_PaymentTerm_ID)
                WHERE o.isSOTrx ='N' AND o.DocStatus IN ('CO','CL')
                    AND NOT EXISTS (SELECT 1
                                FROM C_OrderLine oll
                                        INNER JOIN C_Order oo ON (oll.C_Order_ID=oo.C_Order_ID)
                                        INNER JOIN C_PaymentTerm opt ON (oo.C_PaymentTerm_ID=opt.C_PaymentTerm_ID)
                                WHERE oll.M_Product_ID = ol.M_Product_ID AND oo.C_BPartner_ID=o.C_BPartner_ID
                                        AND TO_CHAR(oll.DATEORDERED,'MON-YY') = TO_CHAR(ol.DATEORDERED,'MON-YY')
                                        AND opt.NetDays > pt.NetDays
                                )
                    AND ol.M_Product_ID = $1
                    AND ol.DateOrdered >= CAST($2 AS DATE) - INTERVAL '6 months'
                    AND ol.DateOrdered <= $2
                    AND ol.Priceactual > 0
                UNION
                (SELECT TO_CHAR(ol.DATEORDERED,'YYYY') YearOrder,TRUNC(ol.DATEORDERED,'MONTH') MonthOrder,
                        UPPER(bp.Name) Name,
                        pt.NetDays Value,
                        'PO' Source, 'TOP' History
                FROM C_OrderLine ol
                        INNER JOIN C_Order o ON (ol.C_Order_ID=o.C_Order_ID)
                    INNER JOIN C_BPartner bp ON (o.C_BPartner_ID = bp.C_BPartner_ID)
                        INNER JOIN C_PaymentTerm pt ON (o.C_PaymentTerm_ID=pt.C_PaymentTerm_ID)
                WHERE o.isSOTrx ='N' AND o.DocStatus IN ('CO','CL')
                    AND NOT EXISTS (SELECT 1
                                FROM C_OrderLine oll
                                        INNER JOIN C_Order oo ON (oll.C_Order_ID=oo.C_Order_ID)
                                        INNER JOIN C_PaymentTerm opt ON (oo.C_PaymentTerm_ID=opt.C_PaymentTerm_ID)
                                WHERE oll.M_Product_ID = ol.M_Product_ID AND oo.C_BPartner_ID=o.C_BPartner_ID
                                        AND TO_CHAR(oll.DATEORDERED,'MON-YY') = TO_CHAR(ol.DATEORDERED,'MON-YY')
                                        AND opt.NetDays > pt.NetDays
                                )
                    AND ol.M_Product_ID = $1
                    AND ol.DateOrdered < CAST($2 AS DATE) - INTERVAL '6 months' -- Logic > 6 diubah menjadi <
                    AND ol.DateOrdered <= $2
                    AND ol.Priceactual > 0
                    ORDER BY o.DateOrdered Desc
                    LIMIT 1)`;

            const result = await dbClient.query(query, [productId, dateOrdered]);

            if (result.rows.length === 0) {
                return {
                    success: false,
                    message: `No price history detail found for orderId ${productId}`,
                    meta: { count: 0 },
                    data: []
                };

            }

            return {
                success: true,
                message: 'Price history detail fetched successfully',
                meta: { count: result.rows.length },
                data: result.rows
            }
        } catch (error) {
            console.error('Error in getPurchaseOrders:', error);
            return [];
        } finally {
            if (dbClient) {
                await dbClient.release();
            }
        }
    }

    async getPriceHistoryDetailV2(server, productId, limit) {
        let dbClient;
        try {
            dbClient = await server.pg.connect();

            const query = `
            WITH
            ProductOrderLines AS (
                SELECT
                    ol.c_orderline_id, ol.c_order_id, p.m_product_id,
                    p.name AS "ProductName", ol.priceactual, ol.qtyordered, ol.linenetamt
                FROM C_OrderLine ol
                JOIN C_Order o ON ol.c_order_id = o.c_order_id
                JOIN M_Product p ON ol.m_product_id = p.m_product_id
                WHERE ol.m_product_id = $1 AND o.issotrx = 'N'
            ),
            ReceiptData AS (
                SELECT
                    pol.c_orderline_id, SUM(iol.movementqty) AS "TotalReceivedQty", MAX(io.movementdate) AS "LastReceiptDate"
                FROM M_InOutLine iol
                JOIN M_InOut io ON iol.m_inout_id = io.m_inout_id
                JOIN ProductOrderLines pol ON iol.c_orderline_id = pol.c_orderline_id
                GROUP BY pol.c_orderline_id
            )
            SELECT
                pol."ProductName", bp.name AS "SupplierName", o.documentno AS "PoNumber",
                o.dateordered AS "PoDate", cur.iso_code AS "Currency", pol.priceactual AS "PricePerUnit",
                pol.qtyordered AS "Quantity", pt.netdays AS "TermOfPaymentDays", pol.linenetamt AS "TotalPrice",
                -- COALESCE tidak lagi diperlukan karena INNER JOIN menjamin baris ini tidak akan pernah NULL
                rd."TotalReceivedQty" AS "ReceivedQty",
                rd."LastReceiptDate"
            FROM ProductOrderLines pol
            JOIN C_Order o ON pol.c_order_id = o.c_order_id
            JOIN C_BPartner bp ON o.c_bpartner_id = bp.c_bpartner_id
            LEFT JOIN C_PaymentTerm pt ON o.c_paymentterm_id = pt.c_paymentterm_id
            LEFT JOIN C_Currency cur ON o.c_currency_id = cur.c_currency_id
            -- ==================== PERUBAHAN UTAMA DI SINI ====================
            -- Mengubah LEFT JOIN menjadi INNER JOIN untuk memfilter hanya PO yang memiliki receipt
            INNER JOIN ReceiptData rd ON pol.c_orderline_id = rd.c_orderline_id
            -- ===============================================================
            ORDER BY o.dateordered DESC
            LIMIT $2;`;

            // PERBAIKAN: Gunakan 'limit', bukan 'dateOrdered'
            const result = await dbClient.query(query, [productId, limit]);
            // PERBAIKAN: Definisikan 'rows'
            const { rows } = result;

            // Jika tidak ada data, kembalikan null sebagai sinyal ke handler
            if (rows.length === 0) {
                return null;
            }

            /// --- Logika Transformasi Data ---
            const priceHistory = rows.map(row => {
                const quantity = parseInt(row.Quantity, 10);
                const receivedQty = parseInt(row.ReceivedQty, 10);

                // ==================== PERUBAHAN UTAMA DI SINI ====================
                // Tentukan status receipt berdasarkan perbandingan kuantitas
                let statusReceipt = 'Partial';
                if (receivedQty >= quantity) { // Pakai >= untuk antisipasi over-receipt
                    statusReceipt = 'Full';
                }
                // ===============================================================

                return {
                    poNumber: row.PoNumber,
                    poDate: row.PoDate,
                    pricePerUnit: parseFloat(row.PricePerUnit),
                    quantity: quantity,
                    receipt: {
                        received: receivedQty,
                        total: quantity
                    },
                    // Tambahkan field baru ke dalam respons
                    statusReceipt: statusReceipt,
                    lastReceiptDate: row.LastReceiptDate,
                    outstandingQty: quantity - receivedQty,
                    termOfPaymentDays: row.TermOfPaymentDays ? parseInt(row.TermOfPaymentDays, 10) : null,
                    totalPrice: parseFloat(row.TotalPrice)
                };
            });

            const totalRecords = priceHistory.length;
            const sums = priceHistory.reduce((acc, item) => {
                acc.totalPriceSum += item.pricePerUnit;
                acc.totalQtySum += item.quantity;
                acc.totalReceiptSum += item.receipt.received;
                acc.totalTopDaysSum += item.termOfPaymentDays || 0;
                return acc;
            }, { totalPriceSum: 0, totalQtySum: 0, totalReceiptSum: 0, totalTopDaysSum: 0 });

            const finalResponse = {
                productInfo: {
                    name: rows[0].ProductName,
                    supplier: rows[0].SupplierName
                },
                summary: {
                    averagePrice: sums.totalPriceSum / totalRecords,
                    averageQty: sums.totalQtySum / totalRecords,
                    averageReceipt: sums.totalReceiptSum / totalRecords,
                    averageTopDays: sums.totalTopDaysSum / totalRecords,
                    currency: rows[0].Currency || 'N/A'
                },
                priceHistory: priceHistory
            };

            return finalResponse;

        } catch (error) {
            console.error('Error in getPurchaseOrders:', error);
            return [];
        } finally {
            if (dbClient) {
                await dbClient.release();
            }
        }
    }

    async byDepartment(server, keyDept) {
        let dbClient;
        try {
            dbClient = await server.pg.connect();

            const query = `
                SELECT co.documentno
                FROM c_order co
                WHERE EXISTS (
                    SELECT 1
                    FROM c_orderline co2
                    JOIN m_requisitionline mr ON mr.c_orderline_id = co2.c_orderline_id
                    JOIN m_requisition mr2 ON mr2.m_requisition_id = mr.m_requisition_id
                    WHERE co2.c_order_id = co.c_order_id -- Korelasi dengan kueri luar
                    AND mr2.createdby IN (SELECT ad_user_id FROM ad_user au WHERE au.title LIKE $1)
                )`;

            const result = await dbClient.query(query, [`%${keyDept}%`]);

            if (result.rows.length === 0) {
                return {
                    success: false,
                    message: `No order found for this dept ${keyDept}`,
                    meta: { count: 0 },
                    data: []
                };

            }

            return {
                success: true,
                message: 'Order by dept fetched successfully',
                meta: { count: result.rows.length },
                data: result.rows
            }
        } catch (error) {
            console.error('Error in byDepartment:', error);
            return [];
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
                    wfa.created, co.documentno,
                    regexp_replace(wfa.textmsg, '^IsApproved=N - ', '') AS textmsg
                FROM AD_WF_Activity wfa
                JOIN c_order co ON co.c_order_id = wfa.record_id
                WHERE
                    wfa.ad_table_id=259 AND wfa.wfstate = 'CA'
                    AND wfa.ad_user_id = $1
                ORDER BY wfa.created DESC
                LIMIT $2 OFFSET $3`;

            const totalCountQuery = `
                SELECT
                        COUNT(*)
                    FROM AD_WF_Activity wfa
                    JOIN c_order co ON co.c_order_id = wfa.record_id
                    WHERE
                        wfa.ad_table_id=259 AND wfa.wfstate = 'CA'
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

async function order(fastify, opts) {
    await fastify.register(fastifyJwt, {
        secret: 'api2025'
    });

    fastify.decorate("authenticate", async function (request, reply) {
        try {
            await request.jwtVerify()
        } catch (err) {
            reply.send(err.message);
        }
    })

    fastify.decorate('order', new Order())
    fastify.register(autoload, {
        dir: join(import.meta.url, 'routes'),
        options: {
            prefix: opts.prefix
        }
    })
}

export default fp(order)