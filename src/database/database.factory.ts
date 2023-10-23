import { SequelizeModuleOptions } from '@nestjs/sequelize';
import { config } from '../config';

export const DatabaseFactory = (): SequelizeModuleOptions => {
  const { DATABASE } = config;

  return {
    autoLoadModels: true,
    database: DATABASE.NAME,
    define: {
      timestamps: false,
    },
    dialect: DATABASE.DIALECT,
    host: DATABASE.HOST,
    logging: false,
    password: DATABASE.PASSWORD,
    port: DATABASE.PORT,
    username: DATABASE.USERNAME,
  };
};
