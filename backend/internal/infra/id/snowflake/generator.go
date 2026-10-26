package snowflake

import (
	"github.com/bwmarrin/snowflake"
)

type Generator struct {
	node *snowflake.Node
}

func NewGenerator(machineID int) (*Generator, error) {
	node, err := snowflake.NewNode(int64(machineID))
	if err != nil {
		return nil, err
	}
	return &Generator{node: node}, nil
}

func (g *Generator) Generate() (string, error) {
	return g.node.Generate().String(), nil
}
