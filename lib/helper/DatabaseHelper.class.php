<?php

namespace Bugbyte\Deployer\Database;

use Bugbyte\Deployer\Exceptions\DeployException;

class Helper
{
    /**
     * Extracts the timestamp out of the filename of a patch
     *
     * @param string $filename
     * @throws \InvalidArgumentException
     * @return integer
     */
    public static function convertFilenameToTimestamp($filename)
    {
        if (preg_match('/sql_(\d{4})(\d{2})(\d{2})_(\d{2})(\d{2})(\d{2})\./', $filename, $matches)) {
            $timestamp = mktime($matches[4], $matches[5], $matches[6], $matches[2], $matches[3], $matches[1]);

            if (strpos($filename, date('Ymd_His', $timestamp)) !== false) {
                return $timestamp;
            }
        }

        throw new \InvalidArgumentException("Can't convert $filename to timestamp");
    }

    /**
     * Check if all files exist and contain the right class and correct sql code
     *
     * @param string $path_prefix
     * @param array $filepaths
     * @throws DeployException
     * @return \SQL_update[]				[filepath => sql_update_object, ...]
     */
    public static function checkFiles($path_prefix, $filepaths)
    {
        $sql_patch_objects = array();

        foreach ($filepaths as $filename)
        {
            $filepath = $path_prefix .'/'. $filename;

            if (!file_exists($filepath)) {
                throw new DeployException("$filepath not found");
            }

            $classname = self::getClassnameFromFilepath($filepath);

            require_once $filepath;

            if (!class_exists($classname)) {
                throw new DeployException("Class $classname not found in $filepath");
            }

            $sql_patch = new $classname();

            if (!$sql_patch instanceof \SQL_update) {
                throw new DeployException("Class $classname doesn't implement SQL_update");
            }

            $up_sql = trim($sql_patch->up());

            if ($up_sql != '' && substr($up_sql, -1) != ';') {
                throw new DeployException("$classname up() code contains queries but doesn't end with ';'");
            }

            $down_sql = trim($sql_patch->down());

            if ($down_sql != '' && substr($down_sql, -1) != ';') {
                throw new DeployException("$classname down() code contains queries but doesn't end with ';'");
            }

            $sql_patch_objects[$filename] = $sql_patch;
        }

        return $sql_patch_objects;
    }

    public static function getClassnameFromFilepath($filepath)
    {
        return str_replace('.class', '', pathinfo($filepath, PATHINFO_FILENAME));
    }
}
