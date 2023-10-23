import * as dotenv from 'dotenv';
import { Dialect } from 'sequelize';

dotenv.config();

export const config = {
  REPORT_QUEUE_NAME: process.env.REPORT_QUEUE_NAME,
  REPORT_QUEUE_URL: process.env.REPORT_QUEUE_URL,
  AWS_REGION: process.env.AWS_REGION,
  DATABASE: {
    DIALECT: process.env.DATABASE_DIALECT as Dialect,
    HOST: process.env.DATABASE_HOST,
    NAME: process.env.DATABASE_NAME,
    PASSWORD: process.env.DATABASE_PASSWORD,
    PORT: Number(process.env.DATABASE_PORT),
    USERNAME: process.env.DATABASE_USERNAME,
  },
};
