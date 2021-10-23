# Overview

Inherits from the list type. Each element is of type Row, and this class controls how a group of rows is managed. 

## Attributes

`byte_size` - the amount of bytes in an instance of RowList, which in essence is simply a group of Rows. 
`tp_bytes` - A map which has as key a tuple for the topic partition, and as value the amount of bytes from that topic partition that are within the group. 

## Methods

`append` - Adds a row to this structure, and adequately updates the instance's attributes.
`pop` -  Removes a row from this structure, and adequaltely updates the instance's attributes.
`extend` - Extends the current instance of a RowList, with another of the same type, adequately updating each attribute with the other's data.