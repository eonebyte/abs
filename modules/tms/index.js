import fp from 'fastify-plugin'
import autoload from '@fastify/autoload'
import { join } from 'desm'

async function tms(fastify, opts) {
    fastify.register(autoload, {
        dir: join(import.meta.url, 'routes'),
        options: {
            prefix: opts.prefix
        }
    })
}

export default fp(tms)