import { Module } from '@nestjs/common';
import { SequelizeModule } from '@nestjs/sequelize';
import { DatabaseFactory } from './database.factory';

@Module({
  imports: [
    SequelizeModule.forRootAsync({
      useFactory: DatabaseFactory,
    }),
  ],
})
export class DatabaseModule {}
