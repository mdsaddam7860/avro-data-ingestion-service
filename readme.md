### Project Initialization & VS Code Setup
- Create Directory: Open your terminal (or VS Code terminal `Ctrl + ``) and run:

```bash
mkdir middleware-server && cd middleware-server
Initialize Project:
```

```bash
npm init -y
npx tsc --init
```
- VS Code Extensions: Ensure you have the ESLint and Prettier extensions installed for production-grade code consistency.

### Necessary Packages
For this specific pipeline, you need packages that handle Avro schemas, ZIP files, and HTTP requests.

Bash
# Core framework and types
```bash
npm install fastify @fastify/multipart zod axios adm-zip avsc dotenv
```

# Dev dependencies
```bash
npm install -D typescript tsx tsup @types/node @types/adm-zip
Fastify: Faster and lower overhead than Express, excellent for "middleware" tasks.
```

- avsc: The most robust Avro parser for Node.js.

- adm-zip: To handle the unzipping process.

 -Zod: For runtime validation of your environment variables and API responses.

### Production Folder Structure
A "Service-Repository" pattern is best here to keep the file conversion logic separate from the server routes.

```Plaintext
middleware-server/
├── src/
│   ├── config/             # Environment validation 
│   ├── routes/             # Webhook endpoints
│   ├── services/           # Business logic (Avro conversion, Unzipping)
│   ├── utils/              # Axios instance, Logger (Pino)
│   └── index.js            # Entry point 
├── .env                    # Secrets (API Keys)
└── package.json
```
