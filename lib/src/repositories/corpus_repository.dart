import 'dart:async';
import 'package:dddart/dddart.dart';
import '../corpus.dart';

/// Interface for a data store repository for Corpuses
abstract class CorpusRepository extends AggregateRootRepository<Corpus> {

  /// Gets a Corpus by its name.
  Future<Corpus> getByName(String name);
}
