import { MigrationInterface, QueryRunner } from "typeorm";

export class FirstContributionView1698253664753 implements MigrationInterface {
  name = "FirstContributionView1698253664753";

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`CREATE MATERIALIZED VIEW "first_contribution" AS
      SELECT DISTINCT ON ("toId", "fromId")
        "toId",
        "fromId",
        "time",
        "id",
        "typeId",
        "amount"
      FROM "event"
      ORDER BY "toId", "fromId", "time" ASC 
      WITH NO DATA;
    `);
    await queryRunner.query(
      `INSERT INTO "typeorm_metadata"("database", "schema", "table", "type", "name", "value") VALUES (DEFAULT, $1, DEFAULT, $2, $3, $4)`,
      [
        "public",
        "MATERIALIZED_VIEW",
        "first_contribution",
        'SELECT DISTINCT ON ("toId", "fromId")\n      "toId",\n      "fromId",\n      "time",\n      "id",\n      "typeId",\n      "amount"\n    FROM "event"\n    ORDER BY "toId", "fromId", "time" ASC \n    WITH NO DATA;',
      ],
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `DELETE FROM "typeorm_metadata" WHERE "type" = $1 AND "name" = $2 AND "schema" = $3`,
      ["MATERIALIZED_VIEW", "first_contribution", "public"],
    );
    await queryRunner.query(`DROP MATERIALIZED VIEW "first_contribution"`);
  }
}
