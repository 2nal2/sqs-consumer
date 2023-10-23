import { Module } from '@nestjs/common';
import { SqsModule } from '@ssut/nestjs-sqs';
import { ReportConsumerService } from './report-consumer.service';
import { config } from '../../config';

@Module({
  imports: [
    SqsModule.register({
      consumers: [
        {
          name: config.REPORT_QUEUE_NAME,
          queueUrl: config.REPORT_QUEUE_URL,
          region: config.AWS_REGION,
        },
      ],
      producers: [],
    }),
  ],
  controllers: [],
  providers: [ReportConsumerService],
})
export class ReportConsumerModule {}
