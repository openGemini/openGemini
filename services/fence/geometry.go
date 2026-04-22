// Copyright 2025 Huawei Cloud Computing Technologies Co., Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this File except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fence

import (
	"fmt"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
	"github.com/openGemini/openGemini/lib/codec"
)

type Geometry interface {
	Build([]float64) error
	Cells() []s2.CellID
	Contains(p s2.LatLng) bool
	Serialize() []byte
}

type Interface interface {
	Serialize() []byte
	UnSerialize(buf []byte) (*Fence, error)
}

const (
	GeometryCircle  = "circle"
	GeometryRect    = "rect"
	GeometryPolygon = "polygon"
)

type Fence struct {
	ID       string
	Typ      string
	Geometry Geometry
}

type Circle struct {
	lat    float64
	lon    float64
	radius float64

	latLon s2.LatLng
	point  s2.Point
}

// Be cautious when using type == nil, as an error may occur
func (f *Fence) Serialize() []byte {

	buf := make([]byte, 0, 56)
	buf = codec.AppendString(buf, f.ID)
	if f.Typ != "" {
		buf = codec.AppendString(buf, f.Typ)
	}
	if f.Geometry != nil {
		buf = append(buf, f.Geometry.Serialize()...)
	}
	return buf
}

func (f *Fence) UnSerialize(buf []byte) (*Fence, error) {
	dec := codec.NewBinaryDecoder(buf)
	fe := &Fence{
		ID:  dec.String(),
		Typ: dec.String(),
		Geometry: &Circle{
			lat:    dec.Float64(),
			lon:    dec.Float64(),
			radius: dec.Float64(),
		},
	}

	return fe, nil

}

func (c *Circle) Build(items []float64) error {
	if len(items) < 3 {
		return fmt.Errorf("invalid circle: %v", items)
	}

	c.lat, c.lon, c.radius = items[0], items[1], items[2]
	c.latLon = s2.LatLngFromDegrees(c.lat, c.lon)
	c.point = s2.PointFromLatLng(c.latLon)
	return nil
}

func (c *Circle) Serialize() []byte {

	buf := make([]byte, 0, 24)
	buf = codec.AppendFloat64(buf, c.lat)
	buf = codec.AppendFloat64(buf, c.lon)
	buf = codec.AppendFloat64(buf, c.radius)

	return buf

}

func (c *Circle) Cells() []s2.CellID {
	region := c.Region()
	return CoverCell(region)
}

func (c *Circle) Region() s2.Region {
	return s2.CapFromCenterAngle(c.point, s1.Angle(c.radius/1000/earthRadiusKm))
}

func (c *Circle) Contains(p s2.LatLng) bool {
	distance := c.latLon.Distance(p)

	return AngleToKm(distance.Radians())*1000 <= c.radius
}

func AngleToKm(v float64) float64 {
	return v * earthRadiusKm
}

func GetLatLonCell(p s2.LatLng) s2.CellID {
	return s2.CellIDFromLatLng(p).Parent(MaxLevel)
}
