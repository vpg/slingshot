<?php
require_once __DIR__ . '/vendor/autoload.php';
use \vpg\slingshot;

/**
 * Elastic hosts configuration
 * Here we work on the same host
 */
$esCnfHash = [
    'from'   => 'pprod-dc1-provider01.dc.vpg.lan:9200',
    'to'   => '192.168.100.100:9200'
];

/**
 * Replaces all docs of the given index
 * Each new doc is returned by enrichDocCallBack which :
 *     -  Adds 'gar' and 'DT' fields
 *     -  Removes 'foo' field 
 */
$enrichDocCallBack = function ($docHash) {
    $docList = [];
    $saleNB = count($docHash['bus']); 
    $docBuIDHash = [
        'fr' => $docHash['id'],
        'uk' => ($docHash['id'] + 100000),
        'it' => ($docHash['id'] + 200000),
        'es' => ($docHash['id'] + 300000),
        'pl' => ($docHash['id'] + 400000),
        'ch' => ($docHash['id'] + 500000),
        'be' => ($docHash['id'] + 600000),
        'nl' => ($docHash['id'] + 700000),
        'de' => ($docHash['id'] + 800000)
    ];
    for( $saleI = 0; $saleI < $saleNB; $saleI++ ) {
        $buSubDoc = $docHash['bus'][$saleI];
        $bu = $buSubDoc['bu'];
        if ($bu == 'br') continue;
        $_id = $docBuIDHash[$bu];
        $buSubDoc['oldId'] = $buSubDoc['id'];
        $buSubDoc['id'] = $_id;
        $buDocHash = [ 'id' => $_id ];
        $buDocHash['bus'] = $buSubDoc;
        $saleList[] = $buDocHash;
    }
    return $saleList;
};

/**
 * Migration params
 * Here we replace all doc on the given index
 */
$migrationConfHash = [
    'from' => ['index' => 'sale', 'type' => 'details'],
    'to'   => ['index' => 'foo', 'type' => 'bar'],
    'bulk' => ['action' => 'index', 'batchSize' => 500],
    //'documentsBatch' => ['batchNb' => 2, 'batchSize' => 50],
    'documentsBatch' => [],
    'withScroll' => true
];

// Query to filter documents to process
$queryHash = [];

// Run the migration
$slingshot = new slingshot\Slingshot($esCnfHash, $migrationConfHash, slingshot\Slingshot::VERBOSITY_DEBUG);
$slingshot->migrate($queryHash, $enrichDocCallBack);
