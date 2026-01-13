export default async function purchaseOrderDone(fastify, opts) {
    fastify.get('/done', async (request, reply) => {
        try {
            const page = parseInt(request.query.page, 10) || 1; // Default ke halaman 1 jika tidak ada
            const pageSize = parseInt(request.query.pageSize, 10) || 10; // Default 10 item per 

            const purchaseOrders = await fastify.order.getPurchaseDone(fastify, page, pageSize, request.query);
            reply.send(purchaseOrders);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });
}