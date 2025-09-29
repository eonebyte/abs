export default async function purchaseOrderOnProgress(fastify, opts) {
    fastify.get('/revactivity', async (request, reply) => {
        try {
            const { orderId } = request.query;
            const revCount = await fastify.order.getPurchaseRevisionActivity(fastify, orderId);
            reply.send(revCount);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });
}