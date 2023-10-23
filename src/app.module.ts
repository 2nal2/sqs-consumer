import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { RepositoryModule } from './repository';
import { DatabaseModule } from './database';
import { ModelsModule } from './models';
import { ReportConsumerModule } from './consumers/report/report-consumer.module';

@Module({
  imports: [
    DatabaseModule,
    ReportConsumerModule,
    RepositoryModule,
    ModelsModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
