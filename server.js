import { build } from './app.js'
import closeWithGrace from 'close-with-grace'
import dotenv from 'dotenv'
import fs from 'fs';
import { join } from 'desm'

dotenv.config()

const opts = {
    logger: {
        level: 'info'
    },
    https: {
        key: fs.readFileSync(join(import.meta.url, 'ssl', 'adyawinsa.com.key')),
        cert: fs.readFileSync(join(import.meta.url, 'ssl', 'sectigo_adyawinsa.com.crt')),
    }
}

// We want to use pino-pretty only if there is a human watching this,
// otherwise we log as newline-delimited JSON.
if (process.stdout.isTTY) {
    opts.logger.transport = { target: 'pino-pretty' }
}



const port = process.env.BASE_PORT || 3000
const host = process.env.HOST || '0.0.0.0'

const app = await build(opts)
console.log(app.printRoutes())
await app.listen({ port, host })

closeWithGrace(async ({ err }) => {
    if (err) {
        app.log.error({ err }, 'server closing due to error')
    }
    app.log.info('shutting down gracefully')
    await app.close()
})