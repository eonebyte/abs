export default async function supplyRowMaterial(server, opts) {
    server.get('/rev-history', async (request, reply) => {
        try {
            const { userId } = request.query;
            const page = parseInt(request.query.page, 10) || 1; // Default ke halaman 1 jika tidak ada
            const pageSize = parseInt(request.query.pageSize, 10) || 10; // Default 10 item per 

            const revHistories = await server.requisition.getPurchaseRevHistories(server, userId, page, pageSize);
            reply.send(revHistories);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });
}