export default async function purchaseOrderOnProgress(fastify, opts) {
    fastify.get('/flow/progress', async (request, reply) => {
        try {
            const { requisitionId } = request.query;
            const revCount = await fastify.requisition.getFlowProgress(fastify, requisitionId);
            reply.send(revCount);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });
}