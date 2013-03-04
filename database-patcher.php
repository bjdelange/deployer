<?php

//require __DIR__ .'/../../autoload.php';
require __DIR__ .'/includes/patcher_functions.php';
require __DIR__ .'/lib/base/BaseDeploy.class.php';
require __DIR__ .'/lib/Deploy.class.php';
require __DIR__ .'/lib/helper/DatabaseHelper.class.php';
require __DIR__ .'/lib/exceptions/DeployException.class.php';
require __DIR__ .'/lib/interface/SQL_update.class.php';

use Bugbyte\Deployer\Exceptions\DeployException;
use Bugbyte\Deployer\Database\Helper;

if (isset($_SERVER['argv'][1]) && $_SERVER['argv'][1] == 'update' || $_SERVER['argv'][1] == 'rollback') {
    $action = $_SERVER['argv'][1];
} else {
    throw new DeployException('Update or rollback?');
}

if (isset($_SERVER['argv'][2])) {
    $database = intval($_SERVER['argv'][2]);
} else {
    throw new DeployException('Which database?');
}

if (isset($_SERVER['argv'][3])) {
    $timestamp = $_SERVER['argv'][3];
} else {
    throw new DeployException('What is the current timestamp?');
}

if ($_SERVER['argc'] > 4) {
    $patches = array_slice($_SERVER['argv'], 4);
} else {
	throw new DeployException('Which files?');
}

$path = findRootPath($_SERVER['argv'][0], __FILE__);

$sql_patch_objects = Helper::checkFiles($path, $patches);

echo getInstructions($action, $sql_patch_objects, $timestamp);

/**
 * Opent alle classes
 *
 * @param string $action
 * @param SQL_update[] $sql_patch_objects
 * @param integer $timestamp        Pass in the current deployer timestamp to compensate for time differences between deployment source and target servers
 * @return string
 */
function getInstructions($action, $sql_patch_objects, $timestamp)
{
	$sql = '';

	foreach ($sql_patch_objects as $filename => $sql_patch_object)
	{
        if ($action == 'update') {
            $patch_classname = get_class($sql_patch_object);
            $patch_timestamp = Helper::convertFilenameToTimestamp($filename);

            // register the patch in the db_patches table (except for the db_patches table patch itself)
            if ($patch_classname != 'sql_19700101_080000') {
                $sql .= "INSERT INTO db_patches (patch_name, patch_timestamp) VALUES ('$filename', $patch_timestamp);". PHP_EOL;
            }

            // apply the patch
            $sql .= $sql_patch_object->up() . PHP_EOL;

			// if there were no errors, mark the patch as applied
            if ($patch_classname != 'sql_19700101_080000') {
                $sql .= "UPDATE db_patches SET applied_at = FROM_UNIXTIME($timestamp) WHERE patch_name='$filename';". PHP_EOL;
            } else {
                $sql .= "INSERT INTO db_patches (patch_name, patch_timestamp, applied_at) VALUES ('$filename', $patch_timestamp, FROM_UNIXTIME($timestamp));". PHP_EOL;
            }
        }
        elseif ($action == 'rollback') {
            // mark the patch as being reverted
            $sql .= "UPDATE db_patches SET reverted_at = FROM_UNIXTIME($timestamp) WHERE patch_name='$filename';". PHP_EOL;

            // revert the patch
            $sql .= $sql_patch_object->down() . PHP_EOL;

            // remove the patch from the db_patches table
            $sql .= "DELETE FROM db_patches WHERE patch_name='$filename';". PHP_EOL;
        }
	}

	return $sql;
}
