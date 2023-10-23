import { Global, Module } from '@nestjs/common';
import { SequelizeModule } from '@nestjs/sequelize';
import { ReportQueue } from './report-queue.model';

export const models = [ReportQueue];

@Global()
@Module({
  imports: [SequelizeModule.forFeature(models)],
  exports: [SequelizeModule.forFeature(models)],
})
export class ModelsModule {}
