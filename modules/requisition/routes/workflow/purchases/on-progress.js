export default async function purchaseRequisitionOnProgress(fastify, opts) {
    fastify.get('/onprogress', async (request, reply) => {
        try {
            const { userId, documentNo } = request.query;
            const purchaseOrders = await fastify.requisition.getPurchaseOnProgress(fastify, userId, documentNo);
            reply.send(purchaseOrders);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });
}