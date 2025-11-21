export default async function priceHistoryDetail(server, opts) {
    server.get('/:productId/pricehistorydetail', async (request, reply) => {
        try {
            // const { productId, dateOrdered } = request.query;

            const { productId } = request.params;
            const limit = request.query.limit || 6;

            const priceHistories = await server.requisition.getPriceHistoryDetailV2(server, productId, limit);
            reply.send(priceHistories);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });
}