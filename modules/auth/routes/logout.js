export default async function (fastify, options) {
    fastify.get('/logout/oracle', (request, reply) => {
        // Destroy the session
        request.session.delete();
        reply.send({ success: true, message: 'Logged out successfully' });
    });
}