export default async function priceHistoryDetail(server, opts) {
    server.get('/apply/filter', async (request, reply) => {
        try {
            const token = request.headers.authorization; // <-- "Bearer abc123"

            // token tanpa kata Bearer
            const bearerToken = token?.startsWith("Bearer ") ? token.slice(7) : token;
            const wh = await server.requisition.getFilteredRequisition(server, request.query, bearerToken);
            reply.send(wh);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });
}