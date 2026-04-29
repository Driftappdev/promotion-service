package model

import "time"

func (n *News) Publish(now time.Time) error {
	if n.DeletedAt != nil {
		return ErrNewsDeleted
	}
	if n.Published {
		return ErrAlreadyPublished
	}
	now = now.UTC()
	n.Published = true
	n.PublishedAt = &now
	n.UpdatedAt = now
	return nil
}
