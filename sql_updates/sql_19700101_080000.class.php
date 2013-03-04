<?php

/**
 * Very early timestamp to make sure this patch is executed first because it is needed to register all other patches.
 */
class sql_19700101_080000 implements SQL_update
{
	public function up()
	{
		return "
			CREATE TABLE `db_patches` (
              `patch_name` varchar(400) COLLATE ascii_general_ci NOT NULL,
              `patch_timestamp` INTEGER UNSIGNED NOT NULL,
              `applied_at` datetime DEFAULT NULL,
              `reverted_at` datetime DEFAULT NULL,
              PRIMARY KEY (`patch_name`)
            );
		";
	}

	public function down()
	{
		return "

		";
	}
}
