import 'dart:async';
import 'package:dddart/dddart.dart';
import '../gram.dart';
import '../gram_item.dart';

/// Interface for a data store repository for Grams
abstract class GramRepository<T extends GramItem>
    extends AggregateRootRepository<Gram<T>> {

  /// Gets a Gram by its sequence of Items.
  ///
  /// Items must be comparable for equality.
  Future<Gram<T>> getByItems(List<T> items, String corpusId);

  /// Gets a Gram by the first items in its sequence.
  ///
  /// Items must be comparable for equality.
  Future<Gram<T>> getByTextPrefix(List<T> items, List<String> corpusIds);

  /// Gets a random starting gram
  Future<Gram<T>> getRandomStartingGram(List<String> corpusIds);
}
