export default async function userChoice(fastify, opts) {
    fastify.post('/userchoice', async (request, reply) => {
        const { orderId, choiceValue } = request.body;
        try {
            const purchaseOrders = await fastify.order.setUserChoice(request, orderId, choiceValue);
            reply.send(purchaseOrders);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });
}