export default async function supplyRowMaterial(server, opts) {
    server.get('/pricehistory', async (request, reply) => {
        try {
            const { orderId } = request.query;
            const priceHistories = await server.order.getPriceHistory(server, orderId);
            reply.send(priceHistories);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });

    server.get('/:productId/pricehistorydetail', async (request, reply) => {
        try {
            // const { productId, dateOrdered } = request.query;

            const { productId } = request.params;
            const limit = request.query.limit || 6;

            const priceHistories = await server.order.getPriceHistoryDetailV2(server, productId, limit);
            reply.send(priceHistories);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });
}