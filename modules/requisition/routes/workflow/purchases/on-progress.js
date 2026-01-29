export default async function purchaseRequisitionOnProgress(fastify, opts) {
    fastify.get('/onprogress', async (request, reply) => {
        try {
            const page = parseInt(request.query.page, 10) || 1; // Default ke halaman 1 jika tidak ada
            const pageSize = parseInt(request.query.pageSize, 10) || 10; // Default 10 item per 
            const purchaseOrders = await fastify.requisition.getPurchaseOnProgress(fastify, request.query, page, pageSize);
            reply.send(purchaseOrders);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });
}