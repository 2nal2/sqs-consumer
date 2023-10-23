import { IsUUID } from 'class-validator';
import { fn } from 'sequelize';
import {
  Column,
  DataType,
  Default,
  Model,
  PrimaryKey,
  Table,
} from 'sequelize-typescript';
import { ReportQueueStatus } from 'src/enums/report-queue-status.enum';
import { ReportPayload } from 'src/types/report-payload';

@Table({
  tableName: 'report_queue',
  underscored: true,
})
export class ReportQueue extends Model<ReportQueue> {
  @Default(fn('uuid_generate_v4'))
  @IsUUID(4)
  @PrimaryKey
  @Column(DataType.UUID)
  id!: string;

  @Column({ type: DataType.JSONB, allowNull: false })
  payload!: ReportPayload;

  @Column({
    type: DataType.ENUM(...Object.values(ReportQueueStatus)),
    defaultValue: ReportQueueStatus.IN_QUEUE,
  })
  status!: ReportQueueStatus;

  @Column({ type: DataType.STRING, allowNull: true })
  reportUrl?: string;
}
