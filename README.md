# slingshot
Elastic Search Mirgration Lib

* Use Scan&scroll ES feature is used to fetch all docs by batch.
* Every doc is given to a business oriented callback func which is responsible for building the bulk instructions
* Then every batches are indexed in the destination index through a bulk POST call. According to the given bulk options. 

What's going to cost the most is the callback func. Specially if you need strong, business oriented, document modifications. 
E.g. if database connections is required for fetch additional data from production databases in orderenrich each indexed document.

### How to use
 * Add it as deps to your composer project
 * Code the call back function
 * Use it in a batch

```PHP
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
```
