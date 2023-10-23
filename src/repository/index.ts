import { Global, Module } from '@nestjs/common';
import { ReportQueueRepository } from './report-queue.repository';

const repositories = [ReportQueueRepository];
@Global()
@Module({
  providers: repositories,
  exports: repositories,
})
export class RepositoryModule {}
