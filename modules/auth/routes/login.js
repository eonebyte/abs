import oracleDB from "../../../configs/dbOracle.js";

export default async function (fastify, options) {
    fastify.post('/login', async (request, reply) => {
        const dbClient = await fastify.pg.connect();
        const { username, password } = request.body;
        try {
            const result = await dbClient.query(
                'SELECT * FROM AD_User WHERE Name = $1 AND Password = $2 AND IsActive = \'Y\'',
                [username, password]
            );

            if (result.rowCount > 0) {
                const user = result.rows[0];

                // Set session with user information
                request.session.set('user', {
                    id: user.ad_user_id,
                    name: user.name,
                });

                reply.send({ success: true, user: { id: user.ad_user_id, name: user.name } });
            } else {
                reply.code(401).send({ success: false, message: 'Invalid credentials' });
            }
        } catch (error) {
            fastify.log.error(error);
            reply.code(500).send({ success: false, message: 'Server error' });
        }
    });

    fastify.post('/login/oracle', async (request, reply) => {
        let connection;
        const { username, password } = request.body;
        try {

            connection = await oracleDB.openConnection();

            const result = await connection.execute(
                'SELECT * FROM AD_User WHERE Name = :username AND Password = :password AND IsActive = \'Y\'',
                { username, password },
                { outFormat: oracleDB.instanceOracleDB.OUT_FORMAT_OBJECT }
            );

            console.log('this result', result);
            console.log('this count', result.rowCount);


            if (result.rows.length > 0) {
                const user = result.rows[0];

                // Set session with user information
                request.session.set('user', {
                    id: user.ad_user_id,
                    name: user.name,
                });

                reply.send({ success: true, user: { id: user.ad_user_id, name: user.name } });
            } else {
                reply.code(401).send({ success: false, message: 'Invalid credentials' });
            }
        } catch (error) {
            fastify.log.error(error);
            reply.code(500).send({ success: false, message: 'Server error' });
        } finally {
            // 💡 Tutup koneksi jika berhasil dibuka
            if (connection) {
                try {
                    await connection.close();
                } catch (closeErr) {
                    fastify.log.error('Error closing Oracle connection:', closeErr);
                }
            }
        }
    });
}