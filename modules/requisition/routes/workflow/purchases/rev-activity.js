export default async function requisitionRevActivity(fastify, opts) {
    fastify.get('/revactivity', async (request, reply) => {
        try {
            const { requisitionId } = request.query;
            const revCount = await fastify.requisition.getPurchaseRevisionActivity(fastify, requisitionId);
            reply.send(revCount);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });
}