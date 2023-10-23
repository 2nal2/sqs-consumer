import { Injectable } from '@nestjs/common';
import { InjectModel } from '@nestjs/sequelize';
import { ReportQueue } from '../models/report-queue.model';

@Injectable()
export class ReportQueueRepository {
  constructor(
    @InjectModel(ReportQueue)
    private reportQueueModel: typeof ReportQueue,
  ) {}

  insert(record: ReportQueue): Promise<ReportQueue> {
    return this.reportQueueModel.create(record);
  }

  findById(id: string): Promise<ReportQueue> {
    return this.reportQueueModel.findOne({
      where: {
        id,
      },
    });
  }

  async update(record: ReportQueue): Promise<boolean> {
    const result = await this.reportQueueModel.update(record, {
      where: {
        id: record.id,
      },
    });

    return result[0] > 0;
  }
}
