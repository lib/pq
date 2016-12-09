package pq

import (
	"database/sql/driver"
	"fmt"
	"strings"
)

type ReplicationConnection struct {
	cn *conn
}

type CreateReplicationSlotResult struct {
	SlotName        string
	ConsistentPoint string
	SnapshotName    string
	OutputPlugin    string
}

func NewReplicationConnection(connString string) (*ReplicationConnection, error) {
	if !strings.Contains(connString, "replication=") {
		connString = fmt.Sprintf("%s replication=database", connString)
	}

	cn, err := Open(connString)
	if err != nil {
		return nil, err
	}

	return &ReplicationConnection{cn.(*conn)}, nil
}

func (c *ReplicationConnection) Close() error {
	return c.cn.Close()
}

func (c *ReplicationConnection) CreateLogicalReplicationSlot(slotName string, outputPlugin string) (*CreateReplicationSlotResult, error) {
	rows, err := c.cn.simpleQuery(fmt.Sprintf("CREATE_REPLICATION_SLOT %s LOGICAL %s", slotName, outputPlugin))
	if err != nil {
		return nil, err
	}

	values := make([]driver.Value, 4)
	err = rows.Next(values)
	if err != nil {
		return nil, err
	}
	rows.Close()

	return &CreateReplicationSlotResult{
		SlotName:        values[0].(string),
		ConsistentPoint: values[1].(string),
		SnapshotName:    values[2].(string),
		OutputPlugin:    values[3].(string),
	}, nil
}
