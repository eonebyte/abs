export default async function userChoice(fastify, opts) {
    fastify.get('/active', {
        onRequest: [fastify.authenticate]
    },
        async (request, reply) => {
            try {
                const purchaseOrders = await fastify.order.getWorkflowActive(request);
                reply.send(purchaseOrders);
            } catch (error) {
                request.log.error(error);
                reply.status(500).send(error.message);
            }
        });
}