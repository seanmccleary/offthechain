import 'dart:async';
import 'dart:io';
import "package:googleapis_auth/auth_io.dart";
import 'package:logging/logging.dart' as log;
import 'datastore_api.dart' as datastore;

/// Some common funcitonality for using Google Cloud Datastore
class GoogleCloudDatastoreService {

  /// The API functionality
  datastore.DatastoreApi api;

  /// The ID of the GCloud project we're using
  final String projectId;

  /// The namespace of the GCloud Datastore entities we're accessing
  final String namespaceId;

  /// A future indicating whether or not the API is ready.
  /// "Ready" meaning it has connected, done its dirty dirty OAuth business, etc.
  Future<dynamic> apiReady;

  static final log.Logger _logger = new log.Logger('GoogleCloudDatastoreService');
  ServiceAccountCredentials _accountCredentials;
  final List<String> _scopes = <String>["https://www.googleapis.com/auth/datastore"];
  AuthClient _client;
  final String _kind;

  /// Instantiates a GoogleCLoudDatastoreService
  GoogleCloudDatastoreService(String privateKeyId, String privateKey,
      String clientEmail, String clientId, this.projectId, this.namespaceId, this._kind) {

    _accountCredentials = new ServiceAccountCredentials.fromJson(<String, String>{
      "private_key_id": privateKeyId,
      "private_key": privateKey,
      "client_email": clientEmail,
      "client_id": clientId,
      "type": "service_account"
    });

    apiReady = clientViaServiceAccount(_accountCredentials, _scopes)
        .then((AutoRefreshingAuthClient client) {
      _client = client;
      api = new datastore.DatastoreApi(_client);
    });

  }

  /// Gets an object representing a Datastore Key
  datastore.Key getKey(String id) {
    final datastore.PathElement pe = new datastore.PathElement()
      ..kind = _kind
      ..name = id;

    return new datastore.Key()
      ..path = <datastore.PathElement>[pe]
      ..partitionId = getPartitionId();
  }

  /// Gets an object representing a Datastore partition ID
  datastore.PartitionId getPartitionId() => new datastore.PartitionId()
    ..namespaceId = namespaceId
    ..projectId = projectId;

  /// Gets a query Filter
  datastore.Filter getFilter(String attribute, String operation, datastore.Value value) {

    final datastore.PropertyReference propertyReference = new datastore.PropertyReference()
      ..name = attribute;

    final datastore.PropertyFilter propertyFilter = new datastore.PropertyFilter()
      ..property = propertyReference
      ..op = operation
      ..value = value;

    return new datastore.Filter()
        ..propertyFilter = propertyFilter;
  }

  /// Gets a Query object
  datastore.Query getQuery(String entityName, datastore.Filter filter) {

    final datastore.KindExpression kindExpression = new datastore.KindExpression()
      ..name = entityName;

    return new datastore.Query()
      ..kind = <datastore.KindExpression>[kindExpression]
      ..filter = filter;
  }

  /// Gets a single Entity from the Datastore
  Future<datastore.Entity> getSingleEntity(datastore.Query query) async {
    final List<datastore.Entity> results = await getEntities(query);
    return results.isEmpty ? null : results.first;
  }

  /// Gets a set of Entities from the Datastore
  Future<List<datastore.Entity>> getEntities(datastore.Query query) async {

    await apiReady;

    final List<datastore.Entity> entities = new List<datastore.Entity>();

    final datastore.ReadOptions readOptions = new datastore.ReadOptions()
      ..readConsistency = "EVENTUAL";

    final datastore.RunQueryRequest request = new datastore.RunQueryRequest()
      ..partitionId = getPartitionId()
      ..readOptions = readOptions
      ..query = query;

    final datastore.RunQueryResponse response = await api.projects.runQuery(
        request, projectId);
    if (response.batch != null && response.batch.entityResults != null &&
        response.batch.entityResults.isNotEmpty) {
      response.batch.entityResults.forEach(
              (datastore.EntityResult entityResult) => entities.add(entityResult.entity));
    }

    return entities;
  }

  /// Tries a function several times, backing off for an increasing amount of time
  /// in event of failure.
  Future<dynamic> tryWithBackoff(Function func) async {
    const int attempts = 10;
    for(int x = 0; x < attempts; x++) {
      try {
        _logger.fine("About to execute function with backoff");
        await func();
        _logger.fine("Executed function with backoff");
        return;
      } catch (e) {
        if (x < (attempts - 1)) {
          _logger.fine("Function failed: ${e.toString()}. Sleeping for $x seconds");
          sleep(new Duration(seconds: x));
        } else {
          _logger.fine("Function failed 10 times: ${e.toString()}. Giving up.");
          rethrow;
        }
      }
    }
  }
}