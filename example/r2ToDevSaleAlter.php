<?php
require_once __DIR__ . '/vendor/autoload.php';
use \vpg\slingshot;

/**
 * Elastic hosts configuration
 * Here we work on the same host
 */
$esCnfHash = [
    'from' => 'vp-ol-provider-rec2.recette.vpg.lan:9200',
    'to'   => '192.168.100.100:9200'
];

/**
 * Replaces all docs of the given index
 * Each new doc is returned by enrichDocCallBack which :
 *     -  Adds 'gar' and 'DT' fields
 *     -  Removes 'foo' field 
 */
$enrichDocCallBack = function ($docHash) {
    $offerNb = count($docHash['bus']); 
    for( $offerI = 0; $offerI < $offerNb; $offerI++ ) {
        $docHash['bus'][$offerI]['nr_of_images'] = count($docHash['bus'][$offerI]['locales'][0]['images']);
    }
    return $docHash;
};

/**
 * Migration params
 * Here we replace all doc on the given index
 */
$migrationConfHash = [
    'from' => ['index' => 'sale', 'type' => 'details', 'size' => 50],
    'to'   => ['index' => 'sale_rc', 'type' => 'details'],
    'bulk' => ['action' => 'index', 'batchSize' => 500],
    'withScroll' => true
];

// Query to filter documents to process
$queryHash = [];

// Run the migration
$slingshot = new slingshot\Slingshot($esCnfHash, $migrationConfHash);
$slingshot->migrate($queryHash, $enrichDocCallBack);
