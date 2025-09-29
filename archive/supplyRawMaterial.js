export default async function supplyRowMaterial(fastify, opts) {
    fastify.get('/supply-rm', async (request, reply) => {
        try {
            const { startDate, endDate } = request.query;
            const productBoms = await fastify.order.calculateRmRequirements(startDate, endDate);
            reply.send(productBoms);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });
}