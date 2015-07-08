<?php
require_once __DIR__ . '/vendor/autoload.php';
use \vpg\slingshot;

/**
 * Elastic hosts configuration
 * Here we work on the same host
 */
$esCnfHash = [
    'from' => '192.168.100.100:9200',
    'to'   => '192.168.100.100:9200'
];

/**
 * Replaces all docs of the given index
 * Each new doc is returned by enrichDocCallBack which :
 *     -  Adds 'gar' and 'DT' fields
 *     -  Removes 'foo' field 
 */
$enrichDocCallBack = function ($docHash) {
    $docHash['far'] = 'FAAAAAAR' . $docHash['_id'];
    $docHash['DT'] = date('Y-m-d H:i:s');
    return $docHash;
};

/**
 * Migration params
 * Here we replace all doc on the given index
 */
$migrationConfHash = [
    'from' => ['index' => 'new_test', 'type' => 'boo'],
    'to'   => ['index' => 'new_test', 'type' => 'boo'],
    'bulk' => ['action' => 'index', 'batchSize' => 3]
];

// Run the migration
$slingshot = new slingshot\Slingshot($esCnfHash, $migrationConfHash);
$slingshot->migrate($queryHash = [], $enrichDocCallBack);
