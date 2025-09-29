import oracleDB from "../../../configs/dbOracle.js";


async function timeIn(fastify, opts) {
    fastify.get('/time/in', async (request, reply) => {
        let connection;
        try {
            connection = await oracleDB.openConnection();

            const query = `
                SELECT 
                mi.M_INOUT_ID,
                atms.ADW_TMS_ID,
                mi.DOCUMENTNO,
                atms.TNKB,
                atms.DRIVER_NAME,
                atms.TIME_IN
                FROM M_INOUT mi
                INNER JOIN ADW_TMS atms ON mi.ADW_TMS_ID = atms.ADW_TMS_ID 
                WHERE TRUNC(atms.TIME_IN) = TRUNC(SYSDATE)
            `;

            const result = await connection.execute(query, {}, {
                outFormat: oracleDB.instanceOracleDB.OUT_FORMAT_OBJECT
            });


            if (result.rows && result.rows.length > 0) {
                reply.send({
                    success: true,
                    count: result.rows.length,
                    data: result.rows,
                });
            } else {
                fastify.log.info("No TIME_IN data for today found.");
                reply.code(404).send({
                    success: false,
                    message: "No data found for TIME_IN today.",
                });
            }

        } catch (error) {
            fastify.log.error(error);
            reply.code(500).send({ success: false, message: 'Server error' });
        } finally {
            if (connection) {
                try {
                    await connection.close();
                } catch (closeErr) {
                    fastify.log.error('Error closing Oracle connection:', closeErr);
                }
            }
        }
    })
}

export default timeIn;