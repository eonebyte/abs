export default async function (server, opts) {
    server.post('/token/refresh', async (request, reply) => {
        const { refreshToken } = request.body;

        if (!refreshToken) {
            return reply.code(400).send({ success: false, message: 'Refresh token is required' });
        }

        try {
            const decoded = await server.jwt.verify(refreshToken);

            if (!decoded.isRefreshToken) {
                return reply.code(401).send({ success: false, message: 'Invalid refresh token' });
            }

            // Remove isRefreshToken before creating new accessToken
            const { isRefreshToken, ...userPayload } = decoded;

            const newAccessToken = server.jwt.sign(userPayload, { expiresIn: '1h' });

            return reply.send({ success: true, token: newAccessToken });

        } catch (err) {
            return reply.code(401).send({ success: false, message: 'Refresh token expired or invalid' });
        }
    });

}


