export default async function supplyRowMaterial(server, opts) {
    server.get('/by-department', async (request, reply) => {
        try {
            const { keyDept } = request.query;
            const priceHistories = await server.order.byDepartment(server, keyDept);
            reply.send(priceHistories);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });
}