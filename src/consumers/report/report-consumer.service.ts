import { Injectable } from '@nestjs/common';
import {
  SqsConsumerEventHandler,
  SqsMessageHandler,
  SqsService,
} from '@ssut/nestjs-sqs';
import * as AWS from '@aws-sdk/client-sqs';
import { config } from '../../config';
import { Message } from '@aws-sdk/client-sqs';
import { ReportQueuePayload } from 'src/types/report-queue-payload';
import { ReportQueueRepository } from 'src/repository/report-queue.repository';
import { ReportQueueStatus } from 'src/enums/report-queue-status.enum';

@Injectable()
export class ReportConsumerService {
  constructor(private readonly reportQueueRepository: ReportQueueRepository) {}
  @SqsMessageHandler(config.REPORT_QUEUE_NAME, false)
  async handleMessage(message: AWS.Message) {
    // parse queue payload
    const queuePayload: ReportQueuePayload = JSON.parse(
      message.Body,
    ) as ReportQueuePayload;

    console.log(`Handling request for report ${queuePayload.id}`);

    // get original report request and mark report in progress
    const report = await this.reportQueueRepository.findById(queuePayload.id);
    report.status = ReportQueueStatus.IN_PROGRESS;
    await this.reportQueueRepository.update(report.toJSON());

    // Do some long running job (e.g get user information from db or some resource
    // and then upload to s3 and get the resource url)
    await new Promise((r) => setTimeout(r, Math.ceil(Math.random() * 10000)));

    // mark report as completed
    report.status = ReportQueueStatus.COMPLETED;
    report.reportUrl = 'https://example.com/report.pdf';
    await this.reportQueueRepository.update(report.toJSON());
  }

  @SqsConsumerEventHandler(config.REPORT_QUEUE_NAME, 'processing_error')
  public async onProcessingError(error: Error, message: Message) {
    console.log(error, message);
    try {
      const payload: ReportQueuePayload = JSON.parse(
        message.Body,
      ) as ReportQueuePayload;

      const job = await this.reportQueueRepository.findById(payload.id);
      job.status = ReportQueueStatus.FAILED;
      await this.reportQueueRepository.update(job);
    } catch (error) {
      // log this error
      console.log(`error handling error`, error);
    }
  }
}
