import fastify from 'fastify'
import autoload from '@fastify/autoload'
import cors from '@fastify/cors';
import fs from 'node:fs';
import { join } from 'desm'
import fastifyStatic from '@fastify/static';


export async function build(opts = {}) {
    const app = fastify(opts)

    const DB_USER_POSTGRES = process.env.DB_USER_POSTGRES;
    const DB_PASSWORD_POSTGRES = process.env.DB_PASSWORD_POSTGRES;
    const DB_NAME_POSTGRES = process.env.DB_NAME_POSTGRES;
    const DB_HOST_POSTGRES = process.env.DB_HOST_POSTGRES;

    await app.register(cors, {
        // origin: `${BASE_URL_CLIENT}:${BASE_PORT_CLIENT}`,
        origin: true,
        credentials: true
    });

    // app.register(import('@fastify/secure-session'), {
    //     sessionName: 'session',
    //     // cookieName: 'my-session-cookie',
    //     key: fs.readFileSync(join(import.meta.url, 'eonebyte')),
    //     expiry: 24 * 60 * 60, // Default 1 day
    //     cookie: {
    //         path: '/',
    //         httpOnly: true
    //         // options for setCookie, see https://github.com/app/app-cookie
    //     }
    // });

    await app.register(await import('@fastify/postgres'), {
        connectionString: `postgres://${DB_USER_POSTGRES}:${DB_PASSWORD_POSTGRES}@${DB_HOST_POSTGRES}/${DB_NAME_POSTGRES}`,
    });
    // Test PostgreSQL connection
    const client = await app.pg.connect();
    await client.query('SELECT NOW()');
    console.log('Connected to PostgreSQL successfully');
    client.release();

    await app.register(fastifyStatic, {
        root: join(import.meta.url, '../client/dist'), // Menyesuaikan path ke folder 'dist'
        prefix: '/', // Semua file di folder dist akan dapat diakses melalui prefix ini
    });

    app.get('/', async (request, reply) => {
        return reply.sendFile('index.html'); // Mengakses file index.html di folder dist
    });

    app.register(autoload, {
        dir: join(import.meta.url, 'modules'),
        encapsulate: false,
        maxDepth: 1,
        options: {
            prefix: '/api/v1'
        }
    })

    app.setErrorHandler(async (err, request, reply) => {
        if (err.validation) {
            reply.code(403)
            return err.message
        }
        request.log.error({ err })
        reply.code(err.statusCode || 500)

        return "I'm sorry, there was an error processing your request."
    })

    app.setNotFoundHandler(async (request, reply) => {
        reply.code(404)
        return "I'm sorry, I couldn't find what you were looking for."
    })

    return app
}