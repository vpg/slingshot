<?php
require_once __DIR__ . '/../vendor/autoload.php';
use \vpg\slingshot;

/**
 * Elastic hosts configuration
 * Here we work on the same host
 */
$esCnfHash = [
    'from' => 'vp-ol-provider-rec2.recette.vpg.lan:9200',
    'to'   => 'vp-provideresearch:9200'
];

/**
 * Replaces all docs of the given index
 * Each new doc is returned by enrichDocCallBack which :
 *     -  Adds 'gar' and 'DT' fields
 *     -  Removes 'foo' field 
 */
$enrichDocCallBack = function ($docHash) {
    return $docHash;
};

/**
 * Migration params
 * Here we replace all doc on the given index
 */
$migrationConfHash = [
    'from' => ['index' => 'sale', 'type' => 'details', 'size' => 50],
    'to'   => ['index' => 'sale', 'type' => 'details'],
    'bulk' => ['action' => 'index', 'batchSize' => 500],
    'documentsBatch' => ['batchNb' => 1, 'batchSize' => 1],
    'withScroll' => true
];

// Query to filter documents not published
$queryHash = json_decode(
    '{
        "nested": {
            "path": "bus",
            "query": {
                "bool": {
                    "must_not": {
                        "match": {
                           "bus.active": "published"
                        }
                    }
                }
            }
        }
    }', true);



// Run the migration
$slingshot = new slingshot\Slingshot($esCnfHash, $migrationConfHash);
$slingshot->migrate($queryHash, $enrichDocCallBack);
