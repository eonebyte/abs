export default async function purchaseOrderSuspend(fastify, opts) {
    fastify.get('/purchases/approved', {
        onRequest: [fastify.authenticate]
    },
        async (request, reply) => {
            try {
                const purchaseOrders = await fastify.order.getPurchaseApproved(fastify);
                reply.send(purchaseOrders);
            } catch (error) {
                request.log.error(error);
                reply.status(500).send(error.message);
            }
        });
}