import 'package:dddart/dddart.dart';

/// A corpus, or data source, for chain creation.
class Corpus extends AggregateRoot {

  /// The name of this corpus.
  String name;

  /// Instantiates a [Corpus].
  Corpus(this.name, {String id = null, DateTime created, DateTime updated})
      : super(id: id, created: created, updated: updated);
}
