import fp from "fastify-plugin";
import autoload from "@fastify/autoload";
import { join } from "desm";
import https from "https";
import axios from "axios";

const baseUrlIdempiere = process.env.BASE_URL_IDEMPIERE;

class Requisition {
  async getPurchaseOnProgress(server, payload) {
    let dbClient;
    try {
      dbClient = await server.pg.connect();

      const {
        userId, // Ini adalah legalizedByid yang wajib
        documentNo,
        m_warehouse_id,
        priority,
        createdby,
        docstatus,
        ad_role_id
      } = payload;

      if (!userId) {
        throw new Error("Parameter userId (legalizedByid) wajib diisi");
      }

      // 1. Inisialisasi Clauses untuk Filter Dinamis
      // Filter dasar: client, org, dan legalizedByid (sesuai logic awal Anda)
      const whereClauses = [
        `mr.ad_client_id = 1000003`,
        `mr.ad_org_id = 1000003`,
        `fa.legalizedbyid = $1`
      ];
      const values = [userId];

      // 2. Tambahkan Filter Dinamis
      if (documentNo) {
        values.push(`%${documentNo}%`);
        whereClauses.push(`mr.documentno ILIKE $${values.length}`);
      }

      if (m_warehouse_id) {
        values.push(m_warehouse_id);
        whereClauses.push(`mr.m_warehouse_id = $${values.length}`);
      }

      if (priority) {
        values.push(priority);
        whereClauses.push(`mr.priorityrule = $${values.length}`);
      }

      if (createdby) {
        values.push(createdby);
        whereClauses.push(`mr.createdby = $${values.length}`);
      }

      // Status default 'IP' jika tidak ditentukan
      if (docstatus) {
        values.push(docstatus);
        whereClauses.push(`mr.docstatus = $${values.length}`);
      } else {
        whereClauses.push(`mr.docstatus = 'IP'`);
      }

      // Filter by Role (Mencari user yang memiliki role tertentu)
      if (ad_role_id) {
        const roleUsersQuery = `
                SELECT aur.ad_user_id
                FROM ad_user_roles aur
                WHERE aur.ad_role_id = $1
            `;
        const resultRole = await dbClient.query(roleUsersQuery, [ad_role_id]);
        const userIds = resultRole.rows.map((r) => parseInt(r.ad_user_id));

        if (userIds.length > 0) {
          const placeholders = userIds.map((_, i) => `$${values.length + i + 1}`).join(", ");
          whereClauses.push(`mr.createdby IN (${placeholders})`);
          values.push(...userIds);
        } else {
          // Jika role diisi tapi tidak ada usernya, langsung return kosong
          return { success: true, message: "No data found for this role", meta: { count: 0 }, data: [] };
        }
      }

      const finalWhere = whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";

      // 3. Query Utama dengan CTE
      const query = `
            WITH wf_activities AS (
                SELECT DISTINCT ON (wfa.record_id, au.ad_user_id) wfa.record_id, wfa.created, au.name AS user_name, au.ad_user_id
                FROM ad_wf_activity wfa
                JOIN ad_user au ON wfa.ad_user_id = au.ad_user_id
                WHERE wfa.ad_wf_node_id > 1000000 AND wfa.wfstate = 'CC' AND wfa.ad_table_id = 702
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
                    MAX(CASE WHEN h.aborted THEN NULL WHEN r.nourut = 2 THEN r.user_name END) AS legalizedby,
                    MAX(CASE WHEN h.aborted THEN NULL WHEN r.nourut = 2 THEN r.ad_user_id END) AS legalizedbyid,
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
            ${finalWhere}
            AND fa.preparedby IS NOT NULL
            AND fa.legalizedby IS NOT NULL
            ORDER BY mr.created DESC
        `;

      const result = await dbClient.query(query, values);

      return {
        success: true,
        message: "Purchase orders workflow on progress fetched successfully",
        meta: { count: result.rowCount },
        data: result.rows.map((row) => ({
          record_id: Number(row.m_requisition_id),
          documentNo: row.documentno,
          description: row.description,
          docStatus: row.docstatus,
          preparedBy: row.preparedby,
          legalizedBy: row.legalizedby,
          approvedBy: row.approvedby,
        })),
      };
    } catch (error) {
      server.log.error(error);
      return {
        success: false,
        message: "Failed to fetch purchase orders",
        errors: [error.message],
        data: [],
      };
    } finally {
      if (dbClient) await dbClient.release();
    }
  }

  async getPurchaseDone(server, payload) {
    let dbClient;
    try {
      dbClient = await server.pg.connect();

      const {
        documentNo,
        m_warehouse_id,
        priority,
        createdby,
        docstatus,
        ad_role_id
      } = payload;

      // 1. Inisialisasi Clauses untuk Filter Dinamis
      // Filter dasar: client, org, dan legalizedByid (sesuai logic awal Anda)
      const whereClauses = [
        `mr.ad_client_id = 1000003`,
        `mr.ad_org_id = 1000003`,
        `fa.preparedby IS NOT NULL`,
        `fa.legalizedby IS NOT NULL`,
        `fa.approvedby IS NOT NULL`
      ];

      const values = [];

      // 2. Tambahkan Filter Dinamis
      if (documentNo) {
        values.push(`%${documentNo}%`);
        whereClauses.push(`mr.documentno ILIKE $${values.length}`);
      }

      if (m_warehouse_id) {
        values.push(m_warehouse_id);
        whereClauses.push(`mr.m_warehouse_id = $${values.length}`);
      }

      if (priority) {
        values.push(priority);
        whereClauses.push(`mr.priorityrule = $${values.length}`);
      }

      if (createdby) {
        values.push(createdby);
        whereClauses.push(`mr.createdby = $${values.length}`);
      }

      // Status default 'IP' jika tidak ditentukan
      if (docstatus) {
        values.push(docstatus);
        whereClauses.push(`mr.docstatus = $${values.length}`);
      } else {
        whereClauses.push(`mr.docstatus = 'CO'`);
      }

      // Filter by Role (Mencari user yang memiliki role tertentu)
      if (ad_role_id) {
        const roleUsersQuery = `
                SELECT aur.ad_user_id
                FROM ad_user_roles aur
                WHERE aur.ad_role_id = $1
            `;
        const resultRole = await dbClient.query(roleUsersQuery, [ad_role_id]);
        const userIds = resultRole.rows.map((r) => parseInt(r.ad_user_id));

        if (userIds.length > 0) {
          const placeholders = userIds.map((_, i) => `$${values.length + i + 1}`).join(", ");
          whereClauses.push(`mr.createdby IN (${placeholders})`);
          values.push(...userIds);
        } else {
          // Jika role diisi tapi tidak ada usernya, langsung return kosong
          return { success: true, message: "No data found for this role", meta: { count: 0 }, data: [] };
        }
      }

      const finalWhereString = whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";

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
                ${finalWhereString}
                    `;

      const result = await dbClient.query(query, values);

      return {
        success: true,
        message: "Purchase orders workflow on progress fetched successfully",
        meta: { count: result.rowCount },
        data: result.rows.map((row) => ({
          record_id: Number(row.m_requisition_id),
          documentNo: row.documentno,
          description: row.description,
          plant: row.plant,
          vendor: row.vendor,
          dateOrdered: row.dateordered,
          docStatus: row.docstatus,
          preparedBy: row.preparedby,
          legalizedBy: row.legalizedby,
          approvedBy: row.approvedby,
        })),
      };
    } catch (error) {
      server.log.error(error);
      return {
        success: false,
        message: "Failed to fetch purchase orders workflow on progress",
        errors: [error.message],
        data: [],
      };
    } finally {
      if (dbClient) {
        await dbClient.release();
      }
    }
  }

  async getPurchaseRevHistories(server, page, pageSize, payload) {
    let dbClient;
    try {
      dbClient = await server.pg.connect();

      const { userId, documentNo, m_warehouse_id, ad_role_id, priority, createdby } = payload;
      const offset = (page - 1) * pageSize;

      // 1. Inisialisasi Query Dasar & Parameter
      // Kita masukkan userId sebagai parameter pertama ($1)
      let values = [userId];
      let whereClauses = [
        "wfa.ad_user_id = $1",
        "wfa.ad_table_id = 702",
        "wfa.wfstate = 'CA'"
      ];

      // 2. Filter Dinamis (Mulai dari $2 dan seterusnya)
      if (documentNo) {
        values.push(`%${documentNo}%`);
        whereClauses.push(`mr.documentno ILIKE $${values.length}`);
      }

      if (m_warehouse_id) {
        values.push(m_warehouse_id);
        whereClauses.push(`mr.m_warehouse_id = $${values.length}`);
      }

      if (priority) {
        values.push(priority);
        whereClauses.push(`mr.priorityrule = $${values.length}`);
      }

      if (createdby) {
        values.push(createdby);
        whereClauses.push(`mr.createdby = $${values.length}`);
      }

      // Filter by Role
      if (ad_role_id) {
        const roleUsersQuery = `SELECT ad_user_id FROM ad_user_roles WHERE ad_role_id = $1`;
        const resultRole = await dbClient.query(roleUsersQuery, [ad_role_id]);
        const userIds = resultRole.rows.map((r) => parseInt(r.ad_user_id));

        if (userIds.length > 0) {
          const placeholders = userIds.map((_, i) => `$${values.length + i + 1}`).join(", ");
          whereClauses.push(`mr.createdby IN (${placeholders})`);
          values.push(...userIds);
        } else {
          return { success: true, message: "No data found for this role", meta: { count: 0 }, data: [] };
        }
      }

      const finalWhereString = `WHERE ${whereClauses.join(" AND ")}`;

      // 3. Query Total Count (Harus pakai WHERE yang sama agar pagination sinkron)
      const totalCountQuery = `
            SELECT COUNT(*) 
            FROM AD_WF_Activity wfa
            JOIN m_requisition mr ON mr.m_requisition_id = wfa.record_id
            ${finalWhereString}
        `;

      // 4. Query Data
      // Tambahkan limit dan offset di akhir array values
      const dataValues = [...values];
      dataValues.push(pageSize);
      const limitIdx = dataValues.length;
      dataValues.push(offset);
      const offsetIdx = dataValues.length;

      const dataQuery = `
            SELECT
                wfa.record_id,
                wfa.created, 
                mr.documentno,
                regexp_replace(wfa.textmsg, '^IsApproved=N - ', '') AS textmsg
            FROM AD_WF_Activity wfa
            JOIN m_requisition mr ON mr.m_requisition_id = wfa.record_id
            ${finalWhereString}
            ORDER BY wfa.created DESC
            LIMIT $${limitIdx} OFFSET $${offsetIdx}
        `;

      // Jalankan Query
      const [result, totalCountResult] = await Promise.all([
        dbClient.query(dataQuery, dataValues),
        dbClient.query(totalCountQuery, values),
      ]);

      const totalCount = parseInt(totalCountResult.rows[0].count, 10);

      return {
        success: true,
        message: result.rows.length > 0 ? "Rev History fetched successfully" : "Rev History not found",
        meta: {
          total: totalCount,
          count: result.rows.length,
          per_page: pageSize,
          current_page: page,
          total_pages: Math.ceil(totalCount / pageSize),
        },
        data: result.rows,
      };

    } catch (error) {
      console.error("Error in Rev History:", error);
      return {
        success: false,
        message: "Internal Server Error",
        meta: { count: 0 },
        data: []
      };
    } finally {
      if (dbClient) await dbClient.release();
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
                    WHERE wfa.ad_wf_node_id > 1000000
                    AND wfa.wfstate = 'CC' AND wfa.ad_table_id = 702 -- requisition
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
                    mr.m_requisition_id,
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
                FROM M_Requisition mr
                LEFT JOIN final_approvers fa ON fa.record_id = mr.m_requisition_id
                WHERE mr.ad_client_id = 1000003
                AND mr.ad_org_id = 1000003
                AND mr.m_requisition_id = $1`;

      const result = await dbClient.query(query, [orderId]);

      return {
        success: true,
        message: "Purchase orders workflow on progress fetched successfully",
        meta: { count: result.rowCount },
        data: result.rows.map((row) => ({
          ...row,
          c_order_id: Number(row.c_order_id),
        })),
      };
    } catch (error) {
      server.log.error(error);
      return {
        success: false,
        message: "Failed to fetch purchase orders workflow on progress",
        errors: [error.message],
        data: [],
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
                    wfp.ad_table_id = 702
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
                            wfa.ad_table_id = 702
                            AND wfa.ad_wf_process_id = $1
                            AND wfa.textmsg LIKE 'IsApproved=%'
                            ORDER BY updated ASC
                    `;
        const detailRes = await dbClient.query(detailQuery, [
          row.ad_wf_process_id,
        ]);

        const activities = detailRes.rows
          .filter((r) => r.status_msg !== null)
          .map((r) => ({
            msg: r.status_msg,
            dateActivity: r.updated,
            userName: r.name,
            userRole: r.title,
          }));

        data.push({
          norev: row.norev,
          wf_process_id: row.ad_wf_process_id,
          activity: activities,
        });
      }

      return { data };
    } catch (error) {
      server.log.error(error);
      return {
        success: false,
        message: "Failed to fetch purchase orders workflow on progress",
        errors: [error.message],
        data: [],
      };
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
      const priceHistory = rows.map((row) => {
        const quantity = parseInt(row.Quantity, 10);
        const receivedQty = parseInt(row.ReceivedQty, 10);

        // ==================== PERUBAHAN UTAMA DI SINI ====================
        // Tentukan status receipt berdasarkan perbandingan kuantitas
        let statusReceipt = "Partial";
        if (receivedQty >= quantity) {
          // Pakai >= untuk antisipasi over-receipt
          statusReceipt = "Full";
        }
        // ===============================================================

        return {
          poNumber: row.PoNumber,
          poDate: row.PoDate,
          pricePerUnit: parseFloat(row.PricePerUnit),
          quantity: quantity,
          receipt: {
            received: receivedQty,
            total: quantity,
          },
          // Tambahkan field baru ke dalam respons
          statusReceipt: statusReceipt,
          lastReceiptDate: row.LastReceiptDate,
          outstandingQty: quantity - receivedQty,
          termOfPaymentDays: row.TermOfPaymentDays
            ? parseInt(row.TermOfPaymentDays, 10)
            : null,
          totalPrice: parseFloat(row.TotalPrice),
        };
      });

      const totalRecords = priceHistory.length;
      const sums = priceHistory.reduce(
        (acc, item) => {
          acc.totalPriceSum += item.pricePerUnit;
          acc.totalQtySum += item.quantity;
          acc.totalReceiptSum += item.receipt.received;
          acc.totalTopDaysSum += item.termOfPaymentDays || 0;
          return acc;
        },
        {
          totalPriceSum: 0,
          totalQtySum: 0,
          totalReceiptSum: 0,
          totalTopDaysSum: 0,
        },
      );

      const finalResponse = {
        productInfo: {
          name: rows[0].ProductName,
          supplier: rows[0].SupplierName,
        },
        summary: {
          averagePrice: sums.totalPriceSum / totalRecords,
          averageQty: sums.totalQtySum / totalRecords,
          averageReceipt: sums.totalReceiptSum / totalRecords,
          averageTopDays: sums.totalTopDaysSum / totalRecords,
          currency: rows[0].Currency || "N/A",
        },
        priceHistory: priceHistory,
      };

      return finalResponse;
    } catch (error) {
      console.error("Error in getPurchaseOrders:", error);
      return [];
    } finally {
      if (dbClient) {
        await dbClient.release();
      }
    }
  }

  async getWarehouse(server) {
    let dbClient;
    try {
      dbClient = await server.pg.connect();

      const query = `
            SELECT wh.m_warehouse_id, wh."name"
            FROM m_warehouse wh WHERE wh.ad_client_id IN (1000000, 1000003)`;

      const result = await dbClient.query(query);

      return result.rows.map((row) => ({
        m_warehouse_id: parseInt(row.m_warehouse_id),
        name: row.name,
      }));
    } catch (error) {
      console.error("Error in getWarehouse:", error);
      return [];
    } finally {
      if (dbClient) {
        await dbClient.release();
      }
    }
  }

  async getRole(server) {
    let dbClient;
    try {
      dbClient = await server.pg.connect();

      const query = `
            SELECT ad_role_id, name FROM AD_Role
            WHERE ismasterrole = 'N' AND ad_client_id IN (1000000, 1000003)
            AND name NOT ILIKE '%admin%'
            AND name NOT ILIKE '%user%'`;

      const result = await dbClient.query(query);

      return result.rows.map((row) => ({
        ad_role_id: parseInt(row.ad_role_id),
        name: row.name,
      }));
    } catch (error) {
      console.error("Error in getWarehouse:", error);
      return [];
    } finally {
      if (dbClient) {
        await dbClient.release();
      }
    }
  }

  async getPriorityRule(server) {
    let dbClient;
    try {
      dbClient = await server.pg.connect();

      const query = `
            SELECT value, name, description FROM AD_Ref_List WHERE ad_reference_id=154 AND ad_client_id = 0 ORDER BY value`;

      const result = await dbClient.query(query);

      return result.rows.map((row) => ({
        value: row.value,
        name: row.name,
        description: row.description,
      }));
    } catch (error) {
      console.error("Error in getWarehouse:", error);
      return [];
    } finally {
      if (dbClient) {
        await dbClient.release();
      }
    }
  }

  async getCreatedBy(server) {
    let dbClient;
    try {
      dbClient = await server.pg.connect();

      const query = `
            SELECT DISTINCT u.ad_user_id, u.value, u.name
            FROM ad_user u
            JOIN ad_user_roles ur ON ur.ad_user_id = u.ad_user_id
            JOIN ad_role r ON r.ad_role_id = ur.ad_role_id
            WHERE u.name NOT ILIKE '%admin%'
            AND u.name NOT ILIKE '%user%'
            AND u.name NOT ILIKE '%system%'
            AND u.name NOT ILIKE '%web%'`;

      const result = await dbClient.query(query);

      return result.rows.map((row) => ({
        ad_user_id: parseInt(row.ad_user_id),
        // value: row.value,
        name: row.name,
      }));
    } catch (error) {
      console.error("Error in getWarehouse:", error);
      return [];
    } finally {
      if (dbClient) {
        await dbClient.release();
      }
    }
  }

  async getFilteredRequisition(server, payload, bearerToken) {
    let dbClient;

    try {
      const agent = new https.Agent({ rejectUnauthorized: false });

      // 1. Validasi Token
      if (!bearerToken) {
        throw { statusCode: 401, message: "Missing Bearer Token" };
      }

      // 2. Validasi Parameter Wajib (ad_user_id)
      // Cek di awal agar kode di bawah aman menggunakan variable ad_user_id
      if (!payload || !payload.ad_user_id) {
        throw { statusCode: 400, message: "Parameter ad_user_id wajib diisi" };
      }

      // Destructure payload
      const {
        ad_user_id,
        m_warehouse_id,
        ad_role_id,
        priority,
        createdby,
        docstatus,
      } = payload;

      // 3. Cek apakah ada filter TAMBAHAN selain ad_user_id
      // Kita cek apakah properti filter lain memiliki nilai (truthy)
      const hasExtraFilters =
        m_warehouse_id || ad_role_id || priority || createdby || docstatus;

      // ============================================================
      // KONDISI 1: HANYA AD_USER_ID (Tanpa Filter Lain)
      // Endpoint: .../filter?ad_user_id=1000069
      // Action: Langsung return workflow full dari Axios
      // ============================================================
      if (!hasExtraFilters) {
        const res = await axios.get(
          `${baseUrlIdempiere}/api/v1/workflow/${ad_user_id}`,
          {
            headers: {
              Authorization: `Bearer ${bearerToken}`,
            },
            httpsAgent: agent,
          },
        );

        // Return sesuai struktur yang diminta
        return {
          workflow: res.data.nodes,
        };
      }

      // ============================================================
      // KONDISI 2: ADA FILTER TAMBAHAN
      // Endpoint: .../filter?ad_user_id=...&ad_role_id=...
      // Action: Connect DB -> Filter SQL -> Filter Workflow -> Return Object
      // ============================================================

      // Baru connect DB di sini (Lazy connection)
      dbClient = await server.pg.connect();

      const whereClauses = [];
      const values = [];

      // --- FILTER SQL BUILDER ---

      // Filter by Warehouse
      if (m_warehouse_id) {
        values.push(m_warehouse_id);
        whereClauses.push(`mr.m_warehouse_id = $${values.length}`);
      }

      // Filter by Priority
      if (priority) {
        values.push(priority);
        whereClauses.push(`mr.priorityrule = $${values.length}`);
      }

      // Filter By CreatedBy
      if (createdby) {
        values.push(createdby);
        whereClauses.push(`mr.createdby = $${values.length}`);
      }

      // Filter By DocStatus
      if (docstatus) {
        values.push(docstatus);
        whereClauses.push(`mr.docstatus = $${values.length}`);
      }

      // Filter by Role -> get list of user_ids that belong to role id
      if (ad_role_id) {
        const roleUsersQuery = `
                SELECT aur.ad_user_id
                FROM ad_user au
                JOIN ad_user_roles aur ON au.ad_user_id = aur.ad_user_id
                WHERE au.ad_client_id IN (1000000, 1000003)
                AND au.name NOT ILIKE '%admin%'
                AND au.name NOT ILIKE '%user%'
                AND aur.ad_role_id = $1
            `;
        const resultRole = await dbClient.query(roleUsersQuery, [ad_role_id]);
        const userIds = resultRole.rows.map((r) => parseInt(r.ad_user_id));

        console.log("roles : ", userIds);

        if (userIds.length > 0) {
          // Buat placeholder dinamis ($2, $3, dst)
          const placeholders = userIds
            .map((_, i) => `$${values.length + i + 1}`)
            .join(", ");
          whereClauses.push(`mr.createdby IN (${placeholders})`);
          values.push(...userIds);

          console.log("values : ", values);
        } else {
          // User dengan role tersebut tidak ditemukan, hasil pasti kosong
          return { requisitions: [], workflow: {} };
        }
      }

      // Susun query final
      const finalWhere =
        whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";

      const query = `
            SELECT
                mr.m_requisition_id,
                mr.documentno,
                mr.priorityrule,
                mr.m_warehouse_id,
                mr.createdby,
                mr.created,
                mr.updated
            FROM M_Requisition mr
            ${finalWhere}
            ORDER BY mr.created DESC
            LIMIT 200;
        `;

      const result = await dbClient.query(query, values);

      const requisitions = result.rows.map((row) => ({
        ...row,
        m_requisition_id: parseInt(row.m_requisition_id, 10),
      }));

      console.log("requisition : ", requisitions);

      // --- AXIOS CALL ---
      // Ambil semua workflow aktif dari user
      const res = await axios.get(
        `${baseUrlIdempiere}/api/v1/workflow/${ad_user_id}`,
        {
          headers: {
            Authorization: `Bearer ${bearerToken}`,
          },
          httpsAgent: agent,
        },
      );

      // --- FILTERING NODES (JS) ---
      const reqIds = requisitions.map((r) => r.m_requisition_id);

      const filteredWorkflow = {
        ...res.data,
        // Filter nodes agar hanya menampilkan yang record_id-nya ada di hasil query DB
        nodes:
          res.data.nodes?.filter((node) =>
            reqIds.includes(parseInt(node.record_id)),
          ) || [],
      };

      return {
        // requisitions, // Opsional jika ingin ditampilkan
        workflow: filteredWorkflow.nodes,
      };
    } catch (error) {
      // Error handling dari response Axios
      if (error.response) {
        throw {
          statusCode: error.response.status,
          message:
            error.response.status == 401
              ? "Unauthorized access"
              : error.response.data?.message || "External API Error",
        };
      }

      // Error handling umum
      throw {
        statusCode: error.statusCode || 500,
        message: "server error: " + (error.message || error),
      };
    } finally {
      // Wajib release koneksi DB
      if (dbClient) {
        dbClient.release();
      }
    }
  }
}

async function requisition(fastify, opts) {
  fastify.decorate("requisition", new Requisition());
  fastify.register(autoload, {
    dir: join(import.meta.url, "routes"),
    options: {
      prefix: opts.prefix,
    },
  });
}

export default fp(requisition);
