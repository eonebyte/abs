export default async function purchaseRequisitionDone(fastify, opts) {
    fastify.get('/done', async (request, reply) => {
        try {
            const purchaseOrders = await fastify.requisition.getPurchaseDone(fastify);
            reply.send(purchaseOrders);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });
}