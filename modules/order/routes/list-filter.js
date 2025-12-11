export default async function priceHistoryDetail(server, opts) {
    server.get('/list/warehouse', async (request, reply) => {
        try {
            const wh = await server.requisition.getWarehouse(server);
            reply.send(wh);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });

    server.get('/list/role', async (request, reply) => {
        try {
            const role = await server.requisition.getRole(server);
            reply.send(role);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });

    server.get('/list/priorityrule', async (request, reply) => {
        try {
            const pr = await server.requisition.getPriorityRule(server);
            reply.send(pr);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });

    server.get('/list/createdby', async (request, reply) => {
        try {
            const cd = await server.requisition.getCreatedBy(server);
            reply.send(cd);
        } catch (error) {
            request.log.error(error);
            reply.status(500).send(error.message);
        }
    });

    
}