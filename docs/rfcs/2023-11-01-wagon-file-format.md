---
Feature Name: Wagon File Format
Tracking Issue: TBD
Date: 2023-11-01
Author: "Zhong Zhenchi <zhongzc_arch@outlook.com>"
---

## Abstract 

This document presents a new design, the Wagon file format, created to replace [Iceberg's Puffin format](https://iceberg.apache.org/puffin-spec/) that is implemented in Java and thus not directly usable in our project. The Wagon file format caters for various types of data, with a primary focus on index data.

## 1. Specification

### 1.1 File Layout

The Wagon file format comprises the following components assembled in a sequential structure:

```
[MAGIC]
[Blob₁]
[Blob₂]
...
[Blobₙ]
[Footer]
[MAGIC]
```
Each component is explained in detail in subsequent sections.

### 1.2 MAGIC

The MAGIC component is a unique 4-byte ASCII ('w', 'g', '0', '1') identifier for easy detection of Wagon files.

### 1.3 Blob

Blobs in Wagon can hold varied data types as byte content. The Footer contains the retrieval information for each Blob.

### 1.4 Footer

The Footer maintains metadata about each Blob and consists of `FooterPayload`, `FooterPayloadSize`, and `Flags`.

- `FooterPayload`: This is a JSON object containing two arrays of `BlobMeta` objects and properties:

    ```
    {
      "blob_metas":array,
      "properties":array
    }   
   ```
    - `blob_metas` - An array of `BlobMeta` objects, as follows: 
        ```
        {
          "type":string,
          "offset":number,
          "length":number,
          "compression":string,
          "properties":array
        }   
        ```
        - `type`: A string indicating the Blob data type.
        - `offset`: A numeric value indicating the Blob starting position.
        - `length`: A numeric value indicating the Blob length.
        - `compression`: A string representing the compression type. It could be `"none"`, `"gzip"`, `"lz4"`, `"snappy"`, or `"zstd"`.
        - `properties`: An array of properties related to the Blob. Each property is represented as a key-value pair, with both key and value being strings. These properties offer additional information about the Blob.

    - `properties`: An array of properties represented as key-value pairs. These properties offer additional information about the Footer.

- `FooterPayloadSize`: A 4-byte unsigned integer (little-endian) indicating the `FooterPayload` size.

- `Flags`: 4-bytes to store boolean flags. The first byte's four lower bits indicate the `FooterPayload` compression method amongst "none", "gzip", "lz4", "snappy", or "zstd". Other bits and bytes are set to 0 during writing and are reserved for future use.

## 2. Usage

The Wagon file format can act as a versatile container for diverse types of data. It allows for the custom definition of data types and corresponding encoding-decoding methods. While Wagon is primarily designed for index data, this extendable format can cater to various data requirements.

## 3. Further Work

Future work on the Wagon format entails optimizing the reading and writing of large data quantities, improving index data retention, providing detailed guidelines on acceptable data types and their relevant compression methods, and exploring different Blob formats, reflecting the principles of versatility and scalability inherent to the Wagon design.
