/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.impl.processor;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.function.TriFunction;
import com.hazelcast.jet.impl.pipeline.transform.HashJoinTransform;
import com.hazelcast.jet.impl.processor.HashJoinCollectP.HashJoinArrayList;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.rocksdb.PrefixRocksMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.rocksdb.RocksIterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static java.util.Objects.requireNonNull;

/**
 * Implements the {@linkplain HashJoinTransform hash-join transform}. On
 * all edges except 0 it receives a single item &mdash; the lookup table
 * for that edge (a {@code Map}) and then it processes edge 0 by joining
 * to each item the data from the lookup tables.
 * <p>
 * It extracts a separate key for each of the lookup tables using the
 * functions supplied in the {@code keyFns} argument. Element 0 in that
 * list corresponds to the lookup table received at ordinal 1 and so on.
 * <p>
 * It uses the {@code tags} list to populate the output items. It can be
 * {@code null}, in which case {@code keyFns} must have either one or two
 * elements, corresponding to the two supported special cases in {@link
 * BatchStage} (hash-joining with one or two enriching streams).
 * <p>
 * After looking up all the joined items the processor calls the supplied
 * {@code mapToOutput*Fn} to get the final output item. It uses {@code
 * mapToOutputBiFn} both for the single-arity case ({@code tags == null &&
 * keyFns.size() == 1}) and the variable-arity case ({@code tags != null}).
 * In the latter case the function must expect {@code ItemsByTag} as the
 * second argument. It uses {@code mapToOutputTriFn} for the two-arity
 * case ({@code tags == null && keyFns.size() == 2}).
 */
@SuppressWarnings("unchecked")
@SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
        justification = "https://github.com/spotbugs/spotbugs/issues/844")
public class HashJoinP<E0> extends AbstractProcessor {

    private final List<Function<E0, Object>> keyFns;
    private final List<PrefixRocksMap> lookupTables;
    private final FlatMapper<E0, Object> flatMapper;
    //pool of native iterators, should be made in another way?
    private final List<RocksIterator> iterators;
    private boolean ordinal0Consumed;

    @SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE",
            justification = "https://github.com/spotbugs/spotbugs/issues/844")
    public HashJoinP(
            @Nonnull List<Function<E0, Object>> keyFns,
            @Nonnull List<Tag> tags,
            @Nullable BiFunction mapToOutputBiFn,
            @Nullable TriFunction mapToOutputTriFn,
            @Nullable BiFunctionEx<List<Tag>, Object[], ItemsByTag> tupleToItemsByTag
    ) {
        this.keyFns = keyFns;
        this.lookupTables = new ArrayList<>(Collections.nCopies(keyFns.size(), null));
        this.iterators = new ArrayList<>(Collections.nCopies(keyFns.size(), null));
        BiFunction<E0, Object[], Object> mapTupleToOutputFn;
        checkTrue(mapToOutputBiFn != null ^ mapToOutputTriFn != null,
                "Exactly one of mapToOutputBiFn and mapToOutputTriFn must be non-null");
        if (!tags.isEmpty()) {
            requireNonNull(mapToOutputBiFn, "mapToOutputBiFn required with tags");
            mapTupleToOutputFn = (item, tuple) -> {
                ItemsByTag res = tupleToItemsByTag.apply(tags, tuple);
                return res == null
                        ? null
                        : mapToOutputBiFn.apply(item, res);
            };
        } else if (keyFns.size() == 1) {
            BiFunction mapToOutput = requireNonNull(mapToOutputBiFn,
                    "tags.isEmpty() && keyFns.size() == 1, but mapToOutputBiFn == null");
            mapTupleToOutputFn = (item, tuple) -> mapToOutput.apply(item, tuple[0]);
        } else {
            checkTrue(keyFns.size() == 2, "tags.isEmpty(), but keyFns.size() is neither 1 nor 2");
            TriFunction mapToOutput = requireNonNull(mapToOutputTriFn,
                    "tags.isEmpty() && keyFns.size() == 2, but mapToOutputTriFn == null");
            mapTupleToOutputFn = (item, tuple) -> mapToOutput.apply(item, tuple[0], tuple[1]);
        }

        CombinationsTraverser traverser = new CombinationsTraverser(keyFns.size(), mapTupleToOutputFn);
        flatMapper = flatMapper(traverser::accept);
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        assert !ordinal0Consumed : "Edge 0 must have a lower priority than all other edges";
        //for each map we keep one iterator for that map to limit the number of open iterators
        lookupTables.set(ordinal - 1, (PrefixRocksMap) item);
        iterators.set(ordinal - 1, ((PrefixRocksMap) item).prefixRocksIterator());
        return true;
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        ordinal0Consumed = true;
        return flatMapper.tryProcess((E0) item);
    }

    //This method can, and actually needs to, return null
    //because the iterator can have no values under that key.
    //in which case, the traverser need to skip over the item.
    private Object lookUpJoined(int index, E0 item) {
        PrefixRocksMap<Object, Object> lookupTableForOrdinal = lookupTables.get(index);
        RocksIterator rocksIterator = iterators.get(index);
        Object key = keyFns.get(index).apply(item);
        return getValues(lookupTableForOrdinal.get(rocksIterator, key));
    }

    //Instead of returning a Arraylist from PrefixRocksMap especially for joiner case,
    // we return an iterator over those values and let processors deal with this.
    // A processor may need a different data structure than a list, in that case we need two copies of the same data
    // one for the map and one for the processor, which is wasteful and in extreme cases causes outOfMemoryError.
    // Using the iterator, we only hold one block in memory at a time and let the processor either use it directly
    // or use it to build the data structure it requires.
    private Object getValues(@Nonnull Iterator iterator) {
        Object x = null;
        HashJoinArrayList result = null;
        while (iterator.hasNext()) {
            if (x == null) {
                x = iterator.next();
            } else if (result == null) {
                result = new HashJoinArrayList();
                result.add(x);
                result.add(iterator.next());
            } else {
                result.add(iterator.next());
            }
        }
        //the iterator can have no values under that key, in which case we return null
        if (result == null) {
            return x;
        } else {
            return result;
        }
    }

    private class CombinationsTraverser<OUT> implements Traverser<OUT> {
        private final BiFunction<E0, Object[], OUT> mapTupleToOutputFn;
        private final Object[] lookedUpValues;
        private final int[] indices;
        private final int[] sizes;
        private final Object[] tuple;
        private E0 currentItem;

        CombinationsTraverser(int keyCount, BiFunction<E0, Object[], OUT> mapTupleToOutputFn) {
            this.mapTupleToOutputFn = mapTupleToOutputFn;

            lookedUpValues = new Object[keyCount];
            indices = new int[keyCount];
            sizes = new int[keyCount];
            tuple = new Object[keyCount];
        }

        /**
         * Accept the next item to traverse. The previous item must be fully
         * traversed.
         */
        CombinationsTraverser<OUT> accept(E0 item) {
            assert currentItem == null : "currentItem not null";
            // look up matching values for each joined table
            for (int i = 0; i < lookedUpValues.length; i++) {
                lookedUpValues[i] = lookUpJoined(i, item);
                sizes[i] = lookedUpValues[i] instanceof HashJoinArrayList
                        ? ((ArrayList) lookedUpValues[i]).size() : 1;
            }
            Arrays.fill(indices, 0);
            currentItem = item;
            return this;
        }

        @Override
        public OUT next() {
            while (indices[0] < sizes[0]) {
                // populate the tuple and create the result object
                for (int j = 0; j < lookedUpValues.length; j++) {
                    tuple[j] = sizes[j] == 1 ? lookedUpValues[j]
                            : ((HashJoinArrayList) lookedUpValues[j]).get(indices[j]);
                }
                OUT result = mapTupleToOutputFn.apply(currentItem, tuple);

                // advance indices to the next combination
                for (int j = indices.length - 1; j >= 0; j--) {
                    indices[j]++;
                    if (j == 0 || indices[j] < sizes[j]) {
                        break;
                    }
                    indices[j] = 0;
                }

                if (result != null) {
                    return result;
                }
            }

            currentItem = null;
            return null;
        }
    }
}
