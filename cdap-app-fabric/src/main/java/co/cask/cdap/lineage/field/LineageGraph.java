/*
 * Copyright © 2018 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.lineage.field;

import co.cask.cdap.api.lineage.field.EndPoint;
import co.cask.cdap.api.lineage.field.InputField;
import co.cask.cdap.api.lineage.field.Operation;
import co.cask.cdap.api.lineage.field.ReadOperation;
import co.cask.cdap.api.lineage.field.TransformOperation;
import co.cask.cdap.api.lineage.field.WriteOperation;
import com.google.common.base.Charsets;
import com.google.common.collect.Sets;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents field level lineage graph. Edges of the graph are simply collection of operations.
 * Inputs to the operations and output from it represents the directional flow in the graph.
 * Along with the operations, checksum is also computed for the graph. Checksum algorithm is same as
 * how the Avro computes the Schema fingerprint. https://issues.apache.org/jira/browse/AVRO-1006.
 * First the graph is converted into the canonicalize JSON format, which is used to compute the checksum.
 * The implementation of fingerprint algorithm is taken from {@code org.apache.avro.SchemaNormalization} class.
 * Since the checksum is persisted in store, any change to the canonicalize form or fingerprint algorithm would
 * require upgrade step to update the stored checksums.
 */
public class LineageGraph {
  private final Set<Operation> operations;
  private long checksum;

  /**
   * Create an instance of LineageGraph from the supplied collection of operations.
   * Validations are performed on the collection before creating instance. All of the operations
   * must have unique names. Collection must have at least one operation of type READ and one
   * operation of type WRITE. The origins specified for the {@link InputField} are also validated
   * to make sure the operation with the corresponding name exists in the collection. However, we do not
   * validate the existence of path to the fields in the destination from sources. If no such path exists
   * then the graph will be incomplete.
   *
   * @param operations the collection of operations in the graph
   * @throws IllegalArgumentException if validation fails
   */
  public LineageGraph(Collection<? extends Operation> operations) {
    Set<String> operationNames = new HashSet<>();
    Set<String> allOrigins = new HashSet<>();
    boolean readExists = false;
    boolean writeExists = false;
    for (Operation operation : operations) {
      if (!operationNames.add(operation.getName())) {
        throw new IllegalArgumentException(String.format("All operations provided for creating field " +
                                                           "level lineage graph must have unique names. " +
                                                           "Operation name '%s' is repeated.", operation.getName()));
      }
      switch (operation.getType()) {
        case READ:
          ReadOperation read = (ReadOperation) operation;
          EndPoint source = read.getSource();
          if (source == null) {
            throw new IllegalArgumentException(String.format("Source endpoint cannot be null for the read " +
                                                               "operation '%s'.", read.getName()));
          }
          readExists = true;
          break;
        case TRANSFORM:
          TransformOperation transform = (TransformOperation) operation;
          allOrigins.addAll(transform.getInputs().stream().map(InputField::getOrigin).collect(Collectors.toList()));
          break;
        case WRITE:
          WriteOperation write = (WriteOperation) operation;
          EndPoint destination = write.getDestination();
          if (destination == null) {
            throw new IllegalArgumentException(String.format("Destination endpoint cannot be null for the write " +
                                                               "operation '%s'.", write.getName()));
          }
          allOrigins.addAll(write.getInputs().stream().map(InputField::getOrigin).collect(Collectors.toList()));
          writeExists = true;
          break;
        default:
          // no-op
      }
    }

    if (!readExists) {
      throw new IllegalArgumentException("Field level lineage graph requires at least one operation of type 'READ'.");
    }

    if (!writeExists) {
      throw new IllegalArgumentException("Field level lineage graph requires at least one operation of type 'WRITE'.");
    }

    Sets.SetView<String> invalidOrigins = Sets.difference(allOrigins, operationNames);
    if (!invalidOrigins.isEmpty()) {
      throw new IllegalArgumentException(String.format("No operation is associated with the origins '%s'.",
                                                       invalidOrigins));
    }

    this.operations = new HashSet<>(operations);
    this.checksum = computeChecksum();
  }

  /**
   * @return the checksum for the graph
   */
  public long getChecksum() {
    return checksum;
  }

  /**
   * @return the operations in the graph
   */
  public Set<Operation> getOperations() {
    return operations;
  }

  private long computeChecksum() {
    return fingerprint64(canonicalize().getBytes(Charsets.UTF_8));
  }

  /**
   * Creates the canonicalize representation of the LineageGraph. Canonicalize representation is
   * simply the JSON format of the LineageGraph which includes array of operations.
   * JSON format does not include any SPACES. Before adding the operations to the JSON, they
   * are sorted based on the operation name so that irrespective of the order of insertion, same set
   * of operations always generate same canonicalize form. This representation is then used for
   * computing the graph checksum. So if there are any changes to this representation, upgrade step
   * would be required to update all the checksums stored in store.
   */
  private String canonicalize() {
    StringBuilder builder = new StringBuilder();
    builder.append("{");

    builder.append("\"operations\":[");

    List<Operation> ops = new ArrayList<>(operations);
    Collections.sort(ops, new Comparator<Operation>() {
      @Override
      public int compare(Operation o1, Operation o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });

    for (int i = 0; i <= ops.size() - 1; i++) {
      canonicalize(builder, ops.get(i));
      if (i < ops.size() - 1) {
        builder.append(",");
      }
    }

    builder.append("]");
    builder.append("}");
    return builder.toString();
  }

  /**
   * Append the canonicalize representation of a given {@link Operation} to the builder.
   */
  private void canonicalize(StringBuilder builder, Operation operation) {
    builder.append("{");
    builder.append("\"name\":\"").append(operation.getName()).append("\"").append(",");
    builder.append("\"type\":\"").append(operation.getType().toString().toUpperCase()).append("\"").append(",");
    builder.append("\"description\":\"").append(operation.getDescription()).append("\"").append(",");

    switch (operation.getType()) {
      case READ:
        ReadOperation read = (ReadOperation) operation;
        builder.append("\"source\":");
        canonicalizeEndPoint(builder, read.getSource());
        builder.append(",");
        canonicalizeOutputs(builder, read.getOutputs());
        break;
      case TRANSFORM:
        TransformOperation transform = (TransformOperation) operation;
        canonicalizeInputs(builder, transform.getInputs());
        builder.append(",");
        canonicalizeOutputs(builder, transform.getOutputs());
        break;
      case WRITE:
        WriteOperation write = (WriteOperation) operation;
        canonicalizeInputs(builder, write.getInputs());
        builder.append(",");
        builder.append("\"destination\":");
        canonicalizeEndPoint(builder, write.getDestination());
        break;
      default:
        // no-op
    }
    builder.append("}");
  }

  /**
   * Append the canonicalize representation of a given {@link EndPoint} to the builder.
   */
  private void canonicalizeEndPoint(StringBuilder builder, EndPoint endPoint) {
    builder.append("{");
    builder.append("\"namespace\":\"").append(endPoint.getNamespace()).append("\"");
    builder.append(",");
    builder.append("\"name\":\"").append(endPoint.getName()).append("\"");
    builder.append("}");
  }

  /**
   * Append the canonicalize representation of a list of {@link InputField} to the builder.
   */
  private void canonicalizeInputs(StringBuilder builder, List<InputField> inputs) {
    builder.append("\"inputs\":[");
    for (int i = 0; i <= inputs.size() - 1; i++) {
      builder.append("{");
      builder.append("\"origin\":\"").append(inputs.get(i).getOrigin()).append("\"");
      builder.append(",");
      builder.append("\"name\":\"").append(inputs.get(i).getName()).append("\"");
      builder.append("}");

      if (i < inputs.size() - 1) {
        builder.append(",");
      }
    }
    builder.append("]");
  }

  /**
   * Append the canonicalize representation of a list of outputs to the builder.
   */
  private void canonicalizeOutputs(StringBuilder builder, List<String> outputs) {
    builder.append("\"outputs\":[");
    for (int i = 0; i <= outputs.size() - 1; i++) {
      builder.append("\"").append(outputs.get(i)).append("\"");
      if (i < outputs.size() - 1) {
        builder.append(",");
      }
    }
    builder.append("]");
  }

  private static final long EMPTY64 = 0xc15d213aa4d7a795L;


  /** The implementation of fingerprint algorithm is copied from {@code org.apache.avro.SchemaNormalization} class.
   * Returns the 64-bit Rabin Fingerprint (as recommended in the Avro spec) of a byte string. */
  private long fingerprint64(byte[] data) {
    long result = EMPTY64;
    for (byte b: data) {
      int index = (int) (result ^ b) & 0xff;
      result = (result >>> 8) ^ FP64.FP_TABLE[index];
    }
    return result;
  }

  /* An inner class ensures that FP_TABLE initialized only when needed. */
  private static class FP64 {
    private static final long[] FP_TABLE = new long[256];
    static {
      for (int i = 0; i < 256; i++) {
        long fp = i;
        for (int j = 0; j < 8; j++) {
          long mask = -(fp & 1L);
          fp = (fp >>> 1) ^ (EMPTY64 & mask);
        }
        FP_TABLE[i] = fp;
      }
    }
  }
}
